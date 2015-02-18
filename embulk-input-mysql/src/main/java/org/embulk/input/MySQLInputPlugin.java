package org.embulk.input;

import java.util.Properties;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import com.google.common.base.Throwables;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.mysql.MySQLInputConnection;

public class MySQLInputPlugin
        extends AbstractJdbcInputPlugin
{
    private static final int DEFAULT_PORT = 3306;

    @Override
    protected MySQLInputConnection newConnection(PluginTask task) throws SQLException
    {
        String url = String.format("jdbc:mysql://%s:%d/%s",
                task.getHost(), task.getPort().or(DEFAULT_PORT), task.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", task.getUser());
        props.setProperty("password", task.getPassword());

        // convert 0000-00-00 to NULL to avoid this exceptoin:
        //   java.sql.SQLException: Value '0000-00-00' can not be represented as java.sql.Date
        props.setProperty("zeroDateTimeBehavior", "convertToNull");

        props.setProperty("useCompression", "true");

        props.setProperty("connectTimeout", "300000"); // milliseconds
        props.setProperty("socketTimeout", "1800000"); // smillieconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

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

        if (task.getFetchRows() == 1) {
            logger.info("Fetch size is 1. Fetching rows one by one.");
        } else if (task.getFetchRows() <= 0) {
            logger.info("Fetch size is set to -1. Fetching all rows at once.");
        } else {
            logger.info("Fetch size is {}. Using server-side prepared statement.", task.getFetchRows());
            props.setProperty("useCursorFetch", "true");
        }

        props.putAll(task.getOptions());

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
}
