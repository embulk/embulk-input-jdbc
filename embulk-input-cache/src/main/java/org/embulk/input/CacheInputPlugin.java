package org.embulk.input;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.cache.CacheInputConnection;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;

import com.google.common.base.Optional;
import static java.util.Locale.ENGLISH;

public class CacheInputPlugin
        extends AbstractJdbcInputPlugin
{
    public interface CachePluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("1972")
        public int getPort();

        @Config("namespace")
        public String getNamespace();

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
        return CachePluginTask.class;
    }

    @Override
    protected JdbcInputConnection newConnection(PluginTask task) throws SQLException
    {
        CachePluginTask t = (CachePluginTask) task;

        String url = String.format(ENGLISH, "jdbc:Cache://%s:%d/%s",
                t.getHost(), t.getPort(), t.getNamespace());

        Properties props = new Properties();
        props.setProperty("user", t.getUser());
        if (t.getPassword().isPresent()) {
            props.setProperty("password", t.getPassword().get());
        }
        props.setProperty("connectionTimeout", String.valueOf(t.getConnectTimeout() * 1000)); // milliseconds
        props.setProperty("commandTimeout", String.valueOf(t.getSocketTimeout() * 1000)); // milliseconds
        props.putAll(t.getOptions());

        if (t.getDriverPath().isPresent()) {
            addDriverJarToClasspath(t.getDriverPath().get());
        }

        try {
            Class.forName("com.intersys.jdbc.CacheDriver");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        Connection con = DriverManager.getConnection(url, props);
        try {
            CacheInputConnection c = new CacheInputConnection(con, t.getSchema().orNull());
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

}
