package org.embulk.input;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.oracle.OracleInputConnection;

import com.google.common.base.Optional;

public class OracleInputPlugin
    extends AbstractJdbcInputPlugin
{
    public interface OraclePluginTask
        extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        @ConfigDefault("null")
        public Optional<String> getHost();

        @Config("port")
        @ConfigDefault("1521")
        public int getPort();

        @Config("database")
        @ConfigDefault("null")
        public Optional<String> getDatabase();

        @Config("schema")
        @ConfigDefault("null")
        public Optional<String> getSchema();

        @Config("url")
        @ConfigDefault("null")
        public Optional<String> getUrl();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("tns_admin_path")
        @ConfigDefault("null")
        public Optional<String> getTnsAdminPath();

        @Config("net_service_name")
        @ConfigDefault("null")
        public Optional<String> getNetServiceName();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return OraclePluginTask.class;
    }

    @Override
    protected JdbcInputConnection newConnection(PluginTask task) throws SQLException
    {
        OraclePluginTask oracleTask = (OraclePluginTask) task;

        String url;
        if (oracleTask.getUrl().isPresent()) {
            if (oracleTask.getHost().isPresent() || oracleTask.getDatabase().isPresent()) {
                throw new IllegalArgumentException("'host', 'port' and 'database' parameters are invalid if 'url' parameter is set.");
            }
            if (oracleTask.getNetServiceName().isPresent() || oracleTask.getTnsAdminPath().isPresent()) {
                throw new IllegalArgumentException("'tnsname', 'tnsname_path' parameters are invalid if 'url' parameter is set.");
            }
            url = oracleTask.getUrl().get();
        } else if (oracleTask.getNetServiceName().isPresent() && oracleTask.getTnsAdminPath().isPresent()) {
            if (oracleTask.getHost().isPresent() || oracleTask.getDatabase().isPresent()) {
                throw new IllegalArgumentException("'host' and 'database' parameters are invalid if 'tnsname' and 'tnsname_path' parameters are set.");
            }
            System.setProperty("oracle.net.tns_admin", oracleTask.getTnsAdminPath().get());
            logger.debug(String.format("Setting up env variable oracle.net.tns_admin to be %s", oracleTask.getTnsAdminPath().get()));
            url = String.format("jdbc:oracle:thin:@%s", oracleTask.getNetServiceName().get());
        } else {
            if (!oracleTask.getHost().isPresent()) {
                throw new IllegalArgumentException("Field 'host' is not set.");
            }
            if (!oracleTask.getDatabase().isPresent()) {
                throw new IllegalArgumentException("Field 'database' is not set.");
            }
            url = String.format("jdbc:oracle:thin:@%s:%d:%s",
                    oracleTask.getHost().get(), oracleTask.getPort(), oracleTask.getDatabase().get());
        }

        Properties props = new Properties();
        props.setProperty("user", oracleTask.getUser());
        props.setProperty("password", oracleTask.getPassword());
        props.setProperty("oracle.net.CONNECT_TIMEOUT", String.valueOf(oracleTask.getConnectTimeout() * 1000)); // milliseconds
        props.setProperty("oracle.jdbc.ReadTimeout", String.valueOf(oracleTask.getSocketTimeout() * 1000)); // milliseconds
        props.putAll(oracleTask.getOptions());

        if (oracleTask.getDriverPath().isPresent()) {
            addDriverJarToClasspath(oracleTask.getDriverPath().get());
        }

        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        Connection con = DriverManager.getConnection(url, props);
        try {
            OracleInputConnection c = new OracleInputConnection(con, oracleTask.getSchema().orNull());
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

}
