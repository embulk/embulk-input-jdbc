package org.embulk.input;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.db2.DB2InputConnection;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;

import static java.util.Locale.ENGLISH;

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
        public String getHost();

        @Config("port")
        @ConfigDefault("50000")
        public int getPort();

        @Config("database")
        public String getDatabase();

        @Config("schema")
        @ConfigDefault("null")
        public Optional<String> getSchema();

        @Config("user")
        public String getUser();

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

        String url = String.format(ENGLISH, "jdbc:db2://%s:%d/%s",
                db2Task.getHost(), db2Task.getPort(), db2Task.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", db2Task.getUser());
        if (db2Task.getPassword().isPresent()) {
            props.setProperty("password", db2Task.getPassword().get());
        }
        props.setProperty("connectionTimeout", String.valueOf(db2Task.getConnectTimeout() * 1000)); // milliseconds
        props.setProperty("commandTimeout", String.valueOf(db2Task.getSocketTimeout() * 1000)); // milliseconds
        props.putAll(db2Task.getOptions());
        logConnectionProperties(url, props);

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
            DB2InputConnection c = new DB2InputConnection(con, db2Task.getSchema().orElse(null));
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

}
