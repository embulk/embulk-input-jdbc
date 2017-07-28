package org.embulk.input;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.Ssl;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.mysql.MySQLInputConnection;
import org.embulk.input.mysql.getter.MySQLColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.joda.time.DateTimeZone;

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

        if (t.getDriverPath().isPresent()) {
            addDriverJarToClasspath(t.getDriverPath().get());
        } else {
            try {
                // Gradle test task will add JDBC driver to classpath
                Class.forName("com.mysql.jdbc.Driver");

            } catch (ClassNotFoundException ex) {
                File root = findPluginRoot();
                File driverLib = new File(root, "default_jdbc_driver");
                File[] files = driverLib.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File file) {
                        return file.isFile() && file.getName().endsWith(".jar");
                    }
                });
                if (files == null || files.length == 0) {
                    throw new RuntimeException("Cannot find JDBC driver in '" + root.getAbsolutePath() + "'.");
                } else {
                    for (File file : files) {
                        logger.info("JDBC Driver = " + file.getAbsolutePath());
                        addDriverJarToClasspath(file.getAbsolutePath());
                    }
                }
            }
        }

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

        if (t.getFetchRows() == 1) {
            logger.info("Fetch size is 1. Fetching rows one by one.");
        } else if (t.getFetchRows() <= 0) {
            logger.info("Fetch size is set to -1. Fetching all rows at once.");
        } else {
            logger.info("Fetch size is {}. Using server-side prepared statement.", t.getFetchRows());
            props.setProperty("useCursorFetch", "true");
        }

        props.putAll(t.getOptions());

        // load timezone mappings
        loadTimeZoneMappings();

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }

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
    protected ColumnGetterFactory newColumnGetterFactory(PageBuilder pageBuilder, DateTimeZone dateTimeZone)
    {
        return new MySQLColumnGetterFactory(pageBuilder, dateTimeZone);
    }

    private void loadTimeZoneMappings()
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
        // Here implements a workaround as as workaround.
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
        catch (ClassNotFoundException | IllegalAccessException | NoSuchFieldException | IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if (f != null) {
                f.setAccessible(false);
            }
        }
    }

    @Override
    protected Schema setupTask(JdbcInputConnection con, PluginTask task) throws SQLException
    {
        MySQLInputConnection mySQLCon = (MySQLInputConnection)con;
        mySQLCon.compareTimeZone();
        return super.setupTask(con,task);
    }

}
