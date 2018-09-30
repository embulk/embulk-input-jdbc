package org.embulk.input;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.clickhouse.ClickHouseInputConnection;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;
import ru.yandex.clickhouse.settings.ClickHouseConnectionSettings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

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
        @ConfigDefault("8123")
        public int getPort();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        @Config("database")
        public String getDatabase();

        @Config("buffer_size")
        @ConfigDefault("65536")
        public Optional<Integer> getBufferSize();

        @Config("apache_buffer_size")
        @ConfigDefault("65536")
        public Optional<Integer> getApacheBufferSize();

        @Config("connect_timeout")
        @ConfigDefault("30000")
        public int getConnectTimeout();

        @Config("socket_timeout")
        @ConfigDefault("10000")
        public int getSocketTimeout();

        /**
         * Timeout for data transfer. socketTimeout + dataTransferTimeout is sent to ClickHouse as max_execution_time.
         * ClickHouse rejects request execution if its time exceeds max_execution_time
         */
        @Config("data_transfer_timeout")
        @ConfigDefault("10000")
        public Optional<Integer> getDataTransferTimeout();

        @Config("keep_alive_timeout")
        @ConfigDefault("30000")
        public Optional<Integer> getKeepAliveTimeout();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return ClickHousePluginTask.class;
    }


    @Override
    protected JdbcInputConnection newConnection(PluginTask task) throws SQLException {
        final String DriverClass = "ru.yandex.clickhouse.ClickHouseDriver";

        ClickHousePluginTask t = (ClickHousePluginTask) task;

        loadDriver(DriverClass, t.getDriverPath());

        Properties props = new Properties();
        if (t.getUser().isPresent()) {
            props.setProperty("user", t.getUser().get());
        }
        if (t.getPassword().isPresent()) {
            props.setProperty("password", t.getPassword().get());
        }

        // ClickHouse Connection Options
        if ( t.getApacheBufferSize().isPresent()){
            props.setProperty(ClickHouseConnectionSettings.APACHE_BUFFER_SIZE.getKey(), String.valueOf(t.getApacheBufferSize().get())); // byte?
        }
        if ( t.getBufferSize().isPresent()){
            props.setProperty(ClickHouseConnectionSettings.BUFFER_SIZE.getKey(), String.valueOf(t.getBufferSize().get())); // byte?
        }
        if ( t.getDataTransferTimeout().isPresent() ){
            props.setProperty(ClickHouseConnectionSettings.DATA_TRANSFER_TIMEOUT.getKey(), String.valueOf(t.getDataTransferTimeout().get())); // seconds
        }
        props.setProperty(ClickHouseConnectionSettings.SOCKET_TIMEOUT.getKey(), String.valueOf(t.getSocketTimeout())); // seconds
        props.setProperty(ClickHouseConnectionSettings.CONNECTION_TIMEOUT.getKey(), String.valueOf(t.getConnectTimeout())); // seconds
        props.putAll(t.getOptions());

        final String url = String.format("jdbc:clickhouse://%s:%d/%s", t.getHost(), t.getPort(), t.getDatabase());

        logConnectionProperties(url, props);

        Connection con = DriverManager.getConnection(url, props);
        try {
            ClickHouseInputConnection c = new ClickHouseInputConnection(con, null);
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }
}
