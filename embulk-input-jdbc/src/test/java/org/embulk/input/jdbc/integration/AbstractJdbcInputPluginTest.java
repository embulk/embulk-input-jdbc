package org.embulk.input.jdbc.integration;

import com.google.common.base.Throwables;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractJdbcInputPluginTest
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    protected Connection conn;
    protected ConfigSource config;

    protected Schema schema;

    protected InputPlugin plugin;
    protected MockPageOutput output;

    @Before
    public void createSchemas()
    {
        schema = buildSchema();
    }

    protected abstract Schema buildSchema();

    @Before
    public void createConfigSource()
    {
        config = buildConfigSource(runtime.getExec().newConfigSource());
    }

    protected abstract ConfigSource buildConfigSource(ConfigSource config);

    @Before
    public void createInputPlugin()
    {
        plugin = runtime.getInstance(getInputPluginClass());
    }

    @Before
    public void createMockPageOutput()
    {
        output = new MockPageOutput();
    }

    protected abstract Class<? extends InputPlugin> getInputPluginClass();

    public static Connection setConnection(String jdbcUrl, String user, String password)
    {
        try {
            return DriverManager.getConnection(jdbcUrl, user, password);
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @After
    public void closeConnection()
    {
        if (conn != null) {
            try {
                conn.close();
            }
            catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static void createTable(Connection conn, String tableName, List<String> schema)
    {
        StringBuilder sql = new StringBuilder().append("create table ").append(tableName).append("(");
        for (int i = 0; i < schema.size(); i += 2) {
            if (i != 0) {
                sql.append(", ");
            }
            sql.append(schema.get(i)).append(" ").append(schema.get(i + 1));
        }
        sql.append(");");

        executeSql(conn, sql.toString());
    }

    public static void insertRecord(Connection conn, String tableName, List<String> record)
    {
        StringBuilder sql = new StringBuilder().append("insert into ").append(tableName).append(" values (");
        for (int i = 0; i < record.size(); i++) {
            if (i != 0) {
                sql.append(", ");
            }
            sql.append(record.get(i));
        }
        sql.append(");");

        executeSql(conn, sql.toString());
    }

    public static void dropTable(Connection conn, String tableName)
    {
        executeSql(conn, "drop table if exists " + tableName + ";");
    }

    private static void executeSql(Connection conn, String sql)
    {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public class Control
            implements InputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource, Schema schema, int taskCount)
        {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(plugin.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }
}
