package org.embulk.input;

import java.io.File;
import java.io.FileFilter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.embulk.config.ConfigException;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.postgresql.PostgreSQLInputConnection;
import org.embulk.input.postgresql.getter.PostgreSQLColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLInputPlugin
        extends AbstractJdbcInputPlugin
{
    public interface PostgreSQLPluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("5432")
        public int getPort();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("database")
        public String getDatabase();

        @Config("schema")
        @ConfigDefault("\"public\"")
        public String getSchema();

        @Config("ssl")
        @ConfigDefault("false")
        public boolean getSsl();

        @Config("application_name")
        @ConfigDefault("\"embulk-input-postgresql\"")
        public String getApplicationName();

        @Config("statement_timeout_millis")
        @ConfigDefault("null")
        public Optional<Integer> getStatementTimeoutMillis();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return PostgreSQLPluginTask.class;
    }

    @Override
    protected PostgreSQLInputConnection newConnection(PluginTask task) throws SQLException
    {
        PostgreSQLPluginTask t = (PostgreSQLPluginTask) task;

        this.loadPgJdbcDriver("org.postgresql.Driver", t.getDriverPath());

        String url = String.format("jdbc:postgresql://%s:%d/%s",
                t.getHost(), t.getPort(), t.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", t.getUser());
        props.setProperty("password", t.getPassword());
        props.setProperty("loginTimeout", String.valueOf(t.getConnectTimeout())); // seconds
        props.setProperty("socketTimeout", String.valueOf(t.getSocketTimeout())); // seconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        if (t.getSsl()) {
            // TODO add ssl_verify (boolean) option to allow users to verify certification.
            //      see embulk-input-ftp for SSL implementation.
            props.setProperty("ssl", "true");
            props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");  // disable server-side validation
        }
        // setting ssl=false enables SSL. See org.postgresql.core.v3.openConnectionImpl.

        props.setProperty("ApplicationName", t.getApplicationName());

        props.putAll(t.getOptions());
        logConnectionProperties(url, props);

        Connection con = DriverManager.getConnection(url, props);
        try {
            PostgreSQLInputConnection c = new PostgreSQLInputConnection(con, t.getSchema(), t.getStatementTimeoutMillis());
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    @Override
    protected ColumnGetterFactory newColumnGetterFactory(final PageBuilder pageBuilder, final ZoneId dateTimeZone)
    {
        return new PostgreSQLColumnGetterFactory(pageBuilder, dateTimeZone);
    }

    private Class<? extends java.sql.Driver> loadPgJdbcDriver(
            final String className,
            final Optional<String> driverPath)
    {
        synchronized (pgJdbcDriver) {
            if (pgJdbcDriver.get() != null) {
                return pgJdbcDriver.get();
            }

            try {
                // If the class is found from the ClassLoader of the plugin, that is prioritized the highest.
                final Class<? extends java.sql.Driver> found = loadJdbcDriverClassForName(className);
                pgJdbcDriver.compareAndSet(null, found);

                if (driverPath.isPresent()) {
                    logger.warn(
                            "\"driver_path\" is set while the Pg JDBC driver class \"{}\" is found from the PluginClassLoader."
                                    + " \"driver_path\" is ignored.", className);
                }
                return found;
            }
            catch (final ClassNotFoundException ex) {
                // Pass-through once.
            }

            if (driverPath.isPresent()) {
                logger.info(
                        "\"driver_path\" is set to load the Pg JDBC driver class \"{}\". Adding it to classpath.", className);
                this.addDriverJarToClasspath(driverPath.get());
            }
            else {
                final File root = this.findPluginRoot();
                final File driverLib = new File(root, "default_jdbc_driver");
                final File[] files = driverLib.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(final File file)
                    {
                        return file.isFile() && file.getName().endsWith(".jar");
                    }
                });
                if (files == null || files.length == 0) {
                    throw new ConfigException(new ClassNotFoundException(
                            "The Pg JDBC driver for the class \"" + className + "\" is not found"
                                    + " in \"default_jdbc_driver\" (" + root.getAbsolutePath() + ")."));
                }
                for (final File file : files) {
                    logger.info(
                            "The Pg JDBC driver for the class \"{}\" is expected to be found"
                                    + " in \"default_jdbc_driver\" at {}.", className, file.getAbsolutePath());
                    this.addDriverJarToClasspath(file.getAbsolutePath());
                }
            }

            try {
                // Retrying to find the class from the ClassLoader of the plugin.
                final Class<? extends java.sql.Driver> found = loadJdbcDriverClassForName(className);
                pgJdbcDriver.compareAndSet(null, found);
                return found;
            }
            catch (final ClassNotFoundException ex) {
                throw new ConfigException("The Pg JDBC driver for the class \"" + className + "\" is not found.", ex);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends java.sql.Driver> loadJdbcDriverClassForName(final String className) throws ClassNotFoundException
    {
        return (Class<? extends java.sql.Driver>) Class.forName(className);
    }

    private static final AtomicReference<Class<? extends java.sql.Driver>> pgJdbcDriver = new AtomicReference<>();

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLInputPlugin.class);
}
