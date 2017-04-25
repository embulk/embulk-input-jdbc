package org.embulk.input;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Properties;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

import com.google.common.base.Throwables;
import com.mysql.jdbc.TimeUtil;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.mysql.MySQLInputConnection;
import org.embulk.input.mysql.getter.MySQLColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

public class MySQLInputPlugin
        extends AbstractJdbcInputPlugin
{
    public interface MySQLPluginTask
            extends PluginTask
    {
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

        //
        // TODO
        //
        // The useLegacyDatetimeCode option is obsolete in the MySQL Connector/J 6.
        // It will remove before we upgrade connector-j 6.x for embulk-input-mysql
        //
        // The `useLegacyDatetimeCode=false` option return properly time if server and client timezone is difference.
        // https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-usagenotes-known-issues-limitations.html
        //
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

        String url = String.format("jdbc:mysql://%s:%d/%s",
                t.getHost(), t.getPort(), t.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", t.getUser());
        props.setProperty("password", t.getPassword());

        // convert 0000-00-00 to NULL to avoid this exceptoin:
        //   java.sql.SQLException: Value '0000-00-00' can not be represented as java.sql.Date
        props.setProperty("zeroDateTimeBehavior", "convertToNull");

        props.setProperty("useCompression", "true");

        props.setProperty("connectTimeout", String.valueOf(t.getConnectTimeout() * 1000)); // milliseconds
        props.setProperty("socketTimeout", String.valueOf(t.getSocketTimeout() * 1000)); // milliseconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        // TODO
        // The useLegacyDatetimeCode option is obsolete in the MySQL Connector/J 6.
        // It will remove before we upgrade connector-j 6.x for embulk-input-mysql
        props.setProperty("useLegacyDatetimeCode", String.valueOf(t.getUseLegacyDatetimeCode()));

        // TODO
        //switch task.getSssl() {
        //when "disable":
        //    break;
        //when "enable":
        //    props.setProperty("useSSL", "true");
        //    props.setProperty("requireSSL", "false");
        //    props.setProperty("verifyServerCertificate", "false");
        //    break;
        //when "verify":
        //    props.setProperty("useSSL", "true");
        //    props.setProperty("requireSSL", "true");
        //    props.setProperty("verifyServerCertificate", "true");
        //    break;
        //}

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

        Driver driver;
        try {
            driver = new com.mysql.jdbc.Driver();  // new com.mysql.jdbc.Driver throws SQLException
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        Connection con = driver.connect(url, props);
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
            f = TimeUtil.class.getDeclaredField("timeZoneMappings");
            f.setAccessible(true);

            Properties timeZoneMappings = (Properties) f.get(null);
            if (timeZoneMappings == null) {
                timeZoneMappings = new Properties();
                synchronized (TimeUtil.class) {
                    timeZoneMappings.load(this.getClass().getResourceAsStream("/com/mysql/jdbc/TimeZoneMapping.properties"));
                }
                f.set(null, timeZoneMappings);
            }
        }
        catch (IllegalAccessException | NoSuchFieldException | IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if (f != null) {
                f.setAccessible(false);
            }
        }
    }
}
