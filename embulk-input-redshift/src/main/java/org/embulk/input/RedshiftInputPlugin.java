package org.embulk.input;

import java.util.Properties;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import com.google.common.base.Throwables;
import org.embulk.config.Config;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.postgresql.PostgreSQLInputConnection;

public class RedshiftInputPlugin
        extends AbstractJdbcInputPlugin
{
    private static final String DEFAULT_SCHEMA = "public";
    private static final int DEFAULT_PORT = 5439;

    private static final Driver driver = new org.postgresql.Driver();

    @Override
    protected PostgreSQLInputConnection newConnection(PluginTask task) throws SQLException
    {
        String url = String.format("jdbc:postgresql://%s:%d/%s",
                task.getHost(), task.getPort().or(DEFAULT_PORT), task.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", task.getUser());
        props.setProperty("password", task.getPassword());
        props.setProperty("loginTimeout",   "300"); // seconds
        props.setProperty("socketTimeout", "1800"); // seconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        // TODO
        //switch task.getSssl() {
        //when "disable":
        //    break;
        //when "enable":
        //    props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");  // disable server-side validation
        //when "verify":
        //    props.setProperty("ssl", "true");
        //    break;
        //}

        props.putAll(task.getOptions());

        Connection con = driver.connect(url, props);
        try {
            PostgreSQLInputConnection c = new PostgreSQLInputConnection(con, task.getSchema().or(DEFAULT_SCHEMA));
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }
}
