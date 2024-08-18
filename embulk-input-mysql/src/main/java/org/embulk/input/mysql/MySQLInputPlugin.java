package org.embulk.input.mysql;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.embulk.config.ConfigException;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.Ssl;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.mysql.MySQLInputConnection;
import org.embulk.input.mysql.getter.MySQLColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLInputPlugin
        extends AbstractJdbcInputPlugin
{
    public interface MySQLPluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("3306")
        public int getPort();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("database")
        public String getDatabase();

        @Config("ssl")
        @ConfigDefault("\"disable\"") // backward compatibility
        public Ssl getSsl();

        @Config("use_legacy_datetime_code")
        @ConfigDefault("false")
        public boolean getUseLegacyDatetimeCode();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return MySQLPluginTask.class;
    }

    @Override
    protected MySQLInputConnection newConnection(PluginTask task) throws SQLException
    {
        MySQLPluginTask t = (MySQLPluginTask) task;

        this.loadMySqlJdbcDriver("com.mysql.jdbc.Driver", t.getDriverPath());

        String url = String.format("jdbc:mysql://%s:%d/%s",
                t.getHost(), t.getPort(), t.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", t.getUser());
        props.setProperty("password", t.getPassword());

        // convert 0000-00-00 to NULL to avoid this exception:
        //   java.sql.SQLException: Value '0000-00-00' can not be represented as java.sql.Date
        props.setProperty("zeroDateTimeBehavior", "convertToNull");

        props.setProperty("useCompression", "true");

        props.setProperty("connectTimeout", String.valueOf(t.getConnectTimeout() * 1000)); // milliseconds
        props.setProperty("socketTimeout", String.valueOf(t.getSocketTimeout() * 1000)); // milliseconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        switch (t.getSsl()) {
            case DISABLE:
                props.setProperty("useSSL", "false");
                break;
            case ENABLE:
                props.setProperty("useSSL", "true");
                props.setProperty("requireSSL", "true");
                props.setProperty("verifyServerCertificate", "false");
                break;
            case VERIFY:
                props.setProperty("useSSL", "true");
                props.setProperty("requireSSL", "true");
                props.setProperty("verifyServerCertificate", "true");
                break;
        }

        // NOTE:The useLegacyDatetimeCode option is obsolete in the MySQL Connector/J 6.
        props.setProperty("useLegacyDatetimeCode", String.valueOf(t.getUseLegacyDatetimeCode()));

        if (t.getFetchRows() == 1) {
            logger.info("Fetch size is 1. Fetching rows one by one.");
        } else if (t.getFetchRows() <= 0) {
            logger.info("Fetch size is set to -1. Fetching all rows at once.");
        } else {
            logger.info("Fetch size is {}. Using server-side prepared statement.", t.getFetchRows());
            props.setProperty("useCursorFetch", "true");
        }

        props.putAll(t.getOptions());
        logConnectionProperties(url, props);

        // load timezone mappings
        loadTimeZoneMappingsIfNeeded();

        Connection con = DriverManager.getConnection(url, props);
        try {
            MySQLInputConnection c = new MySQLInputConnection(con);
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
        return new MySQLColumnGetterFactory(pageBuilder, dateTimeZone);
    }

    private void loadTimeZoneMappingsIfNeeded()
    {
        // Here initializes com.mysql.jdbc.TimeUtil.timeZoneMappings static field by calling
        // static timeZoneMappings method using reflection.
        // The field is usually initialized when Driver#connect method is called. But the field
        // initialization fails when a) useLegacyDatetimeCode=false is set AND b) mysql server's
        // default_time_zone is not SYSTEM (default). According to the stacktrace, that's because
        // the com.mysql.jdbc.TimeUtil.loadTimeZoneMappings can't find TimeZoneMapping.properties
        // from the classloader. It seems like a bug of JDBC Driver where it should use the class loader
        // that loaded com.mysql.jdbc.TimeUtil class rather than system class loader to read the
        // property file because the file should be in the same classpath with the class.
        // Here implements a workaround as a workaround.
        //
        // It's not 100% sure, but this workaround is necessary for Connector/J 5.x (com.mysql.jdbc.TimeUtil)
        // only.
        Field f = null;
        try {
            Class<?> timeUtilClass = Class.forName("com.mysql.jdbc.TimeUtil");
            f = timeUtilClass.getDeclaredField("timeZoneMappings");
            f.setAccessible(true);

            Properties timeZoneMappings = (Properties) f.get(null);
            if (timeZoneMappings == null) {
                timeZoneMappings = new Properties();
                synchronized (timeUtilClass) {
                    timeZoneMappings.load(this.getClass().getResourceAsStream("/com/mysql/jdbc/TimeZoneMapping.properties"));
                }
                f.set(null, timeZoneMappings);
            }
        }
        catch (ClassNotFoundException e) {
            // It appears that the user uses the Connector/J 8.x driver.
            // Do nothing;
        }
        catch (IllegalAccessException | NoSuchFieldException | IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (f != null) {
                f.setAccessible(false);
            }
        }
    }

    private Class<? extends java.sql.Driver> loadMySqlJdbcDriver(
            final String className,
            final Optional<String> driverPath)
    {
        synchronized (mysqlJdbcDriver) {
            if (mysqlJdbcDriver.get() != null) {
                return mysqlJdbcDriver.get();
            }

            try {
                // If the class is found from the ClassLoader of the plugin, that is prioritized the highest.
                final Class<? extends java.sql.Driver> found = loadJdbcDriverClassForName(className);
                mysqlJdbcDriver.compareAndSet(null, found);

                if (driverPath.isPresent()) {
                    logger.warn(
                            "\"driver_path\" is set while the MySQL JDBC driver class \"{}\" is found from the PluginClassLoader."
                                    + " \"driver_path\" is ignored.", className);
                }
                return found;
            }
            catch (final ClassNotFoundException ex) {
                // Pass-through once.
            }

            if (driverPath.isPresent()) {
                logger.info(
                        "\"driver_path\" is set to load the MySQL JDBC driver class \"{}\". Adding it to classpath.", className);
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
                            "The MySQL JDBC driver for the class \"" + className + "\" is not found"
                                    + " in \"default_jdbc_driver\" (" + root.getAbsolutePath() + ")."));
                }
                for (final File file : files) {
                    logger.info(
                            "The MySQL JDBC driver for the class \"{}\" is expected to be found"
                                    + " in \"default_jdbc_driver\" at {}.", className, file.getAbsolutePath());
                    this.addDriverJarToClasspath(file.getAbsolutePath());
                }
            }

            try {
                // Retrying to find the class from the ClassLoader of the plugin.
                final Class<? extends java.sql.Driver> found = loadJdbcDriverClassForName(className);
                mysqlJdbcDriver.compareAndSet(null, found);
                return found;
            }
            catch (final ClassNotFoundException ex) {
                throw new ConfigException("The MySQL JDBC driver for the class \"" + className + "\" is not found.", ex);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends java.sql.Driver> loadJdbcDriverClassForName(final String className) throws ClassNotFoundException
    {
        return (Class<? extends java.sql.Driver>) Class.forName(className);
    }

    private static final AtomicReference<Class<? extends java.sql.Driver>> mysqlJdbcDriver = new AtomicReference<>();

    private static final Logger logger = LoggerFactory.getLogger(MySQLInputPlugin.class);
}
