package org.embulk.input;

import java.util.Properties;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import com.google.common.base.Throwables;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.postgresql.PostgreSQLInputConnection;

public class RedshiftInputPlugin
        extends AbstractJdbcInputPlugin
{
    private static final Driver driver = new org.postgresql.Driver();

    public interface RedshiftPluginTask
            extends PluginTask
    {
        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("5439")
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
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return RedshiftPluginTask.class;
    }

    @Override
    protected PostgreSQLInputConnection newConnection(PluginTask task) throws SQLException
    {
        RedshiftPluginTask t = (RedshiftPluginTask) task;

        String url = String.format("jdbc:postgresql://%s:%d/%s",
                t.getHost(), t.getPort(), t.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", t.getUser());
        props.setProperty("password", t.getPassword());
        props.setProperty("loginTimeout",   "300"); // seconds
        props.setProperty("socketTimeout", "1800"); // seconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        // TODO
        //switch t.getSssl() {
        //when "disable":
        //    break;
        //when "enable":
        //    props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");  // disable server-side validation
        //when "verify":
        //    props.setProperty("ssl", "true");
        //    break;
        //}

        props.putAll(t.getOptions());

        Connection con = driver.connect(url, props);
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
}
