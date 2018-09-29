package org.embulk.input;


import com.google.common.base.Optional;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;

import java.sql.SQLException;

public class ClickHouseInputPlugin
        extends AbstractJdbcInputPlugin
{
    public interface ClickHousePluginTask
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
        @ConfigDefault("\"embulk-input-clickhouse\"")
        public String getApplicationName();
    }

    @Override
    protected JdbcInputConnection newConnection(PluginTask task) throws SQLException {
        return null;
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return ClickHousePluginTask.class;
    }

}
