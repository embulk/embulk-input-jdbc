package org.embulk.input;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;

import java.sql.Connection;
import java.sql.Driver;
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

        props.putAll(t.getOptions());

        final String url = String.format("jdbc:clickhouse://%s:%d/%s", t.getHost(), t.getPort(), t.getDatabase());

        Driver driver;
        try {
            // TODO check Class.forName(driverClass) is a Driver before newInstance
            //      for security
            driver = (Driver) Class.forName(DriverClass).newInstance();
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }

        logConnectionProperties(url, props);

        Connection con = driver.connect(url, props);
        try {
            JdbcInputConnection c = new JdbcInputConnection(con, null);
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }
}
