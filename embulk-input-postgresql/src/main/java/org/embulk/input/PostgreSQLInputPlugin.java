package org.embulk.input;

import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.postgresql.PostgreSQLInputConnection;
import org.embulk.input.postgresql.getter.PostgreSQLColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

import com.google.common.base.Optional;

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

        loadDriver("org.postgresql.Driver", t.getDriverPath());

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
            PostgreSQLInputConnection c = new PostgreSQLInputConnection(con, t.getSchema());
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
        return new PostgreSQLColumnGetterFactory(pageBuilder, dateTimeZone);
    }
}
