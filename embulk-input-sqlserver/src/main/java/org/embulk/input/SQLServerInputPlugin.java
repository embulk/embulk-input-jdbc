package org.embulk.input;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.sqlserver.SQLServerInputConnection;

import com.google.common.base.Optional;

public class SQLServerInputPlugin
    extends AbstractJdbcInputPlugin
{
    public interface SQLServerPluginTask
        extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        @ConfigDefault("null")
        public Optional<String> getHost();

        @Config("port")
        @ConfigDefault("1433")
        public int getPort();

        @Config("instance")
        @ConfigDefault("null")
        public Optional<String> getInstance();

        @Config("database")
        @ConfigDefault("null")
        public Optional<String> getDatabase();

        @Config("integratedSecurity")
        @ConfigDefault("null")
        public Optional<Boolean> getIntegratedSecurity();

        @Config("url")
        @ConfigDefault("null")
        public Optional<String> getUrl();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public Optional<String> getPassword();

        @Config("schema")
        @ConfigDefault("null")
        public Optional<String> getSchema();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return SQLServerPluginTask.class;
    }

    @Override
    protected JdbcInputConnection newConnection(PluginTask task) throws SQLException
    {
        SQLServerPluginTask sqlServerTask = (SQLServerPluginTask) task;
        Properties props = new Properties();
        if (sqlServerTask.getUser().isPresent()) {
            props.setProperty("user", sqlServerTask.getUser().get());
        }
        if (sqlServerTask.getPassword().isPresent()) {
            props.setProperty("password", sqlServerTask.getPassword().get());
        }
        props.setProperty("loginTimeout", String.valueOf(sqlServerTask.getConnectTimeout())); // seconds
        props.putAll(sqlServerTask.getOptions());

        if (sqlServerTask.getDriverPath().isPresent()) {
            loadDriverJar(sqlServerTask.getDriverPath().get());
        }
        String url = getUrl(sqlServerTask, getDriverType());

        Connection con = DriverManager.getConnection(url, props);
        try {
            SQLServerInputConnection c = new SQLServerInputConnection(con, sqlServerTask.getSchema().orNull());
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    private String getDriverType()
    {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            return "sqlserver";
        } catch (ClassNotFoundException ex) {}

        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            return "jtds:sqlserver";
        } catch (ClassNotFoundException ex) {}

        throw new RuntimeException("Unknown driver detected.");
    }

    private String getUrl(SQLServerPluginTask sqlServerTask, String driverType)
    {
        String url;
        if (sqlServerTask.getUrl().isPresent()) {
            if (sqlServerTask.getHost().isPresent()
                    || sqlServerTask.getInstance().isPresent()
                    || sqlServerTask.getDatabase().isPresent()
                    || sqlServerTask.getIntegratedSecurity().isPresent()) {
                throw new IllegalArgumentException("'host', 'port', 'instance', 'database' and 'integratedSecurity' parameters are invalid if 'url' parameter is set.");
            }
            return sqlServerTask.getUrl().get();
        }

        if (!sqlServerTask.getHost().isPresent()) {
            throw new IllegalArgumentException("Field 'host' is not set.");
        }
        if (!sqlServerTask.getDatabase().isPresent()) {
            throw new IllegalArgumentException("Field 'database' is not set.");
        }

        StringBuilder urlBuilder = new StringBuilder();
        if (sqlServerTask.getInstance().isPresent()) {
            urlBuilder.append(String.format("jdbc:%s://%s\\%s",
                    driverType, sqlServerTask.getHost().get(), sqlServerTask.getInstance().get()));
        } else {
            urlBuilder.append(String.format("jdbc:%s://%s:%d",
                    driverType, sqlServerTask.getHost().get(), sqlServerTask.getPort()));
        }
        switch(driverType) {
            case "sqlserver":
                if (sqlServerTask.getDatabase().isPresent()) {
                    urlBuilder.append(";databaseName=" + sqlServerTask.getDatabase().get());
                }
                if (sqlServerTask.getIntegratedSecurity().isPresent() && sqlServerTask.getIntegratedSecurity().get()) {
                    urlBuilder.append(";integratedSecurity=" + sqlServerTask.getIntegratedSecurity().get());
                } else {
                    if (!sqlServerTask.getUser().isPresent()) {
                        throw new IllegalArgumentException("Field 'user' is not set.");
                    }
                    if (!sqlServerTask.getPassword().isPresent()) {
                        throw new IllegalArgumentException("Field 'password' is not set.");
                    }
                }
                break;

            case "jtds:sqlserver":
                if (sqlServerTask.getDatabase().isPresent()) {
                    urlBuilder.append("/" + sqlServerTask.getDatabase().get());
                }
                if (sqlServerTask.getIntegratedSecurity().isPresent() && sqlServerTask.getIntegratedSecurity().get()) {
                    throw new IllegalArgumentException("jTDS driver doesn't support integratedSecurity");
                }
                break;
        }
        return urlBuilder.toString();
    }
}
