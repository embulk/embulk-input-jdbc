package org.embulk.input;

import java.util.Set;
import java.util.HashSet;
import java.util.Properties;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;

public class JdbcInputPlugin
        extends AbstractJdbcInputPlugin
{
    private final static Set<String> loadedJarGlobs = new HashSet<String>();

    public interface GenericPluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("driver_class")
        public String getDriverClass();

        @Config("url")
        public String getUrl();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        @Config("schema")
        @ConfigDefault("null")
        public Optional<String> getSchema();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return GenericPluginTask.class;
    }

    @Override
    protected JdbcInputConnection newConnection(PluginTask task) throws SQLException
    {
        GenericPluginTask t = (GenericPluginTask) task;

        if (t.getDriverPath().isPresent()) {
            synchronized (loadedJarGlobs) {
                String glob = t.getDriverPath().get();
                if (!loadedJarGlobs.contains(glob)) {
                    addDriverJarToClasspath(glob);
                    loadedJarGlobs.add(glob);
                }
            }
        }

        Properties props = new Properties();
        if (t.getUser().isPresent()) {
            props.setProperty("user", t.getUser().get());
        }
        if (t.getPassword().isPresent()) {
            props.setProperty("password", t.getPassword().get());
        }

        props.putAll(t.getOptions());

        Driver driver;
        try {
            // TODO check Class.forName(driverClass) is a Driver before newInstance
            //      for security
            driver = (Driver) Class.forName(t.getDriverClass()).newInstance();
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }

        Connection con = driver.connect(t.getUrl(), props);
        try {
            JdbcInputConnection c = new JdbcInputConnection(con, t.getSchema().orNull());
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

}
