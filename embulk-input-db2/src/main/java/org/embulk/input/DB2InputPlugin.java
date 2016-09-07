package org.embulk.input;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.db2.DB2InputConnection;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;

import com.google.common.base.Optional;

public class DB2InputPlugin
    extends AbstractJdbcInputPlugin
{
    public interface DB2PluginTask
        extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        @ConfigDefault("null")
        public Optional<String> getHost();

        @Config("port")
        @ConfigDefault("50000")
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
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return DB2PluginTask.class;
    }

    @Override
    protected JdbcInputConnection newConnection(PluginTask task) throws SQLException
    {
        DB2PluginTask db2Task = (DB2PluginTask) task;

        String url;
        if (db2Task.getUrl().isPresent()) {
            if (db2Task.getHost().isPresent() || db2Task.getDatabase().isPresent()) {
                throw new IllegalArgumentException("'host', 'port' and 'database' parameters are invalid if 'url' parameter is set.");
            }
            url = db2Task.getUrl().get();
        } else {
            if (!db2Task.getHost().isPresent()) {
                throw new IllegalArgumentException("Field 'host' is not set.");
            }
            if (!db2Task.getDatabase().isPresent()) {
                throw new IllegalArgumentException("Field 'database' is not set.");
            }
            url = String.format("jdbc:db2://%s:%d/%s",
                    db2Task.getHost().get(), db2Task.getPort(), db2Task.getDatabase().get());
        }

        Properties props = new Properties();
        if (db2Task.getUser().isPresent()) {
            props.setProperty("user", db2Task.getUser().get());
        }
        if (db2Task.getPassword().isPresent()) {
            props.setProperty("password", db2Task.getPassword().get());
        }
        props.setProperty("connectionTimeout", String.valueOf(db2Task.getConnectTimeout() * 1000)); // milliseconds
        props.setProperty("commandTimeout", String.valueOf(db2Task.getSocketTimeout() * 1000)); // milliseconds
        props.putAll(db2Task.getOptions());

        if (db2Task.getDriverPath().isPresent()) {
            addDriverJarToClasspath(db2Task.getDriverPath().get());
        }

        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        Connection con = DriverManager.getConnection(url, props);
        try {
            DB2InputConnection c = new DB2InputConnection(con, db2Task.getSchema().orNull());
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

}
