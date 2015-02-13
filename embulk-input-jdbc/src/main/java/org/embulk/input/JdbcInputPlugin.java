package org.embulk.input;

import java.util.Properties;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import com.google.common.base.Throwables;
import org.embulk.config.Config;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;

public class JdbcInputPlugin
        extends AbstractJdbcInputPlugin
{
    public interface GenericPluginTask extends PluginTask
    {
        @Config("driver_name")
        public String getDriverName();

        @Config("driver_class")
        public String getDriverClass();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return GenericPluginTask.class;
    }

    @Override
    protected JdbcInputConnection newConnection(PluginTask task) throws SQLException
    {
        GenericPluginTask g = (GenericPluginTask) task;

        String url;
        if (g.getPort().isPresent()) {
            url = String.format("jdbc:%s://%s:%d/%s",
                    g.getDriverName(), g.getHost(), g.getPort().get(), g.getDatabase());
        } else {
            url = String.format("jdbc:%s://%s:%d/%s",
                    g.getDriverName(), g.getHost(), g.getDatabase());
        }

        Properties props = new Properties();
        props.setProperty("user", g.getUser());
        props.setProperty("password", g.getPassword());

        props.putAll(g.getOptions());

        Driver driver;
        try {
            // TODO check Class.forName(driverClass) is a Driver before newInstance
            //      for security
            driver = (Driver) Class.forName(g.getDriverClass()).newInstance();
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }

        Connection con = driver.connect(url, props);
        try {
            JdbcInputConnection c = new JdbcInputConnection(con, g.getSchema().orNull());
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }
}
