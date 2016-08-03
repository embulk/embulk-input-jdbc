package org.embulk.input.jdbc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.embulk.config.Config;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.plugin.PluginClassLoader;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.Exec;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.jdbc.JdbcInputConnection.BatchSelect;
import org.joda.time.DateTimeZone;

public abstract class AbstractJdbcInputPlugin
        implements InputPlugin
{
    protected final Logger logger = Exec.getLogger(getClass());

    public interface PluginTask extends Task
    {
        @Config("options")
        @ConfigDefault("{}")
        public ToStringMap getOptions();

        @Config("table")
        @ConfigDefault("null")
        public Optional<String> getTable();

        @Config("query")
        @ConfigDefault("null")
        public Optional<String> getQuery();

        @Config("select")
        @ConfigDefault("null")
        public Optional<String> getSelect();

        @Config("where")
        @ConfigDefault("null")
        public Optional<String> getWhere();

        @Config("order_by")
        @ConfigDefault("null")
        public Optional<String> getOrderBy();

        @Config("incremental_column")
        @ConfigDefault("null")
        public Optional<List<String>> getIncrementalColumn();

        @Config("last_record")
        @ConfigDefault("null")
        public Optional<Map<String, String>> getLastRecord();
        public void setLastRecord(Optional<Map<String, String>> lastRecord);

        // TODO limit_value is necessary to make sure repeated bulk load transactions
        //      don't a same record twice or miss records when the column
        //      specified at order_by parameter is not unique.
        //      For example, if the order_by column is "timestamp created_at"
        //      column whose precision is second, the table can include multiple
        //      records with the same created_at time. At the first bulk load
        //      transaction, it loads a record with created_at=2015-01-02 00:00:02.
        //      Then next transaction will use WHERE created_at > '2015-01-02 00:00:02'.
        //      However, if another record with created_at=2014-01-01 23:59:59 is
        //      inserted between the 2 transactions, the new record will be skipped.
        //      To prevent this scenario, we want to specify
        //      limit_value=2015-01-02 00:00:00 (exclusive). With this way, as long as
        //      a transaction runs after 2015-01-02 00:00:00 + some minutes, we don't
        //      skip records. Ideally, to automate the scheduling, we want to set
        //      limit_value="today".
        //
        //@Config("limit_value")
        //@ConfigDefault("null")
        //public Optional<String> getLimitValue();

        //// TODO probably limit_rows is unnecessary as long as this has
        //        supports parallel execution (partition_by option) and resuming.
        //@Config("limit_rows")
        //@ConfigDefault("null")
        //public Optional<Integer> getLimitRows();

        @Config("connect_timeout")
        @ConfigDefault("300")
        public int getConnectTimeout();

        @Config("socket_timeout")
        @ConfigDefault("1800")
        public int getSocketTimeout();

        @Config("fetch_rows")
        @ConfigDefault("10000")
        // TODO set minimum number
        public int getFetchRows();

        // TODO parallel execution using "partition_by" config

        @Config("column_options")
        @ConfigDefault("{}")
        public Map<String, JdbcColumnOption> getColumnOptions();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public DateTimeZone getDefaultTimeZone();

        @Config("default_column_options")
        @ConfigDefault("{}")
        public Map<String, JdbcColumnOption> getDefaultColumnOptions();

        @Config("after_select")
        @ConfigDefault("null")
        public Optional<String> getAfterSelect();

        public JdbcSchema getQuerySchema();
        public void setQuerySchema(JdbcSchema schema);

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    // for subclasses to add @Config
    protected Class<? extends PluginTask> getTaskClass()
    {
        return PluginTask.class;
    }

    protected abstract JdbcInputConnection newConnection(PluginTask task) throws SQLException;

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(getTaskClass());

        Schema schema;
        try (JdbcInputConnection con = newConnection(task)) {
            schema = setupTask(con, task);
        } catch (SQLException ex) {
            throw Throwables.propagate(ex);
        }

        return buildNextConfigDiff(task, control.run(task.dump(), schema, 1));
    }

    private Schema setupTask(JdbcInputConnection con, PluginTask task) throws SQLException
    {
        // build SELECT query and gets schema of its result
        String query = getQuery(task, con);
        logger.info("SQL: " + query);
        JdbcSchema querySchema = con.getSchemaOfQuery(query);
        task.setQuerySchema(querySchema);

        // validate column_options
        newColumnGetters(task, querySchema, null);

        ColumnGetterFactory factory = newColumnGetterFactory(null, task.getDefaultTimeZone());
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (int i = 0; i < querySchema.getCount(); i++) {
            JdbcColumn column = querySchema.getColumn(i);
            JdbcColumnOption columnOption = columnOptionOf(task.getColumnOptions(), task.getDefaultColumnOptions(), column, factory.getJdbcType(column.getSqlType()));
            columns.add(new Column(i,
                    column.getName(),
                    factory.newColumnGetter(column, columnOption).getToType()));
        }
        return new Schema(columns.build());
    }

    private String getQuery(PluginTask task, JdbcInputConnection con) throws SQLException
    {
        if (task.getQuery().isPresent()) {
            if (task.getTable().isPresent() || task.getSelect().isPresent() ||
                    task.getWhere().isPresent() || task.getOrderBy().isPresent()) {
                throw new ConfigException("'table', 'select', 'where' and 'order_by' parameters are unnecessary if 'query' parameter is set.");
            } else if (task.getIncrementalColumn().isPresent() || task.getLastRecord().isPresent()) {
                throw new ConfigException("'incremental_column', 'last_record' parameters couldn't be used if 'query' parameter is set.");
            }
            return task.getQuery().get();
        } else if (task.getTable().isPresent()) {
            return con.buildSelectQuery(task.getTable().get(), task.getSelect(),
                    task.getWhere(), task.getOrderBy(), task.getIncrementalColumn(), task.getLastRecord());
        } else {
            throw new ConfigException("'table' parameter is required (if 'query' parameter is not set)");
        }
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(getTaskClass());

        // TODO when parallel execution is implemented and enabled, (maybe) order_by
        //      is necessary to resume. transaction() gets the range of order_by
        //      colum and set it to WHERE condition to make the operation deterministic

        return buildNextConfigDiff(task, control.run(taskSource, schema, taskCount));
    }

    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    protected ConfigDiff buildNextConfigDiff(PluginTask task, List<TaskReport> reports)
    {
        ConfigDiff next = Exec.newConfigDiff();
        if (reports.size() > 0 && reports.get(0).has("last_record")) {
            next.set("last_record", reports.get(0).get(Map.class, "last_record"));
        }
        return next;
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
        // do nothing
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(getTaskClass());

        JdbcSchema querySchema = task.getQuerySchema();
        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        try (JdbcInputConnection con = newConnection(task)) {
            try (BatchSelect cursor = con.newSelectCursor(getQuery(task, con), task.getFetchRows(), task.getSocketTimeout())) {
                List<ColumnGetter> getters = newColumnGetters(task, querySchema, pageBuilder);
                while (true) {
                    // TODO run fetch() in another thread asynchronously
                    // TODO retry fetch() if it failed (maybe order_by is required and unique_column(s) option is also required)
                    boolean cont = fetch(cursor, getters, pageBuilder, task);
                    if (!cont) {
                        break;
                    }
                }
            }
            pageBuilder.finish();

            // after_select runs after pageBuilder.finish because pageBuilder.finish may fail.
            // TODO Output plugin's transaction might still fail. In that case, after_select is
            //      already done but output plugin didn't commit the data to the target storage.
            //      This means inconsistency between data source and destination. To avoid this
            //      issue, we need another option like `after_commit` that runs after output plugin's
            //      commit. after_commit can't run in the same transaction with SELECT. So,
            //      after_select gets values and store them in TaskReport, and after_commit take
            //      them as placeholder. Or, after_select puts values to an intermediate table, and
            //      after_commit moves those values to the actual table.
            if (task.getAfterSelect().isPresent()) {
                con.executeUpdate(task.getAfterSelect().get());
                con.connection.commit();
            }
        } catch (SQLException ex) {
            throw Throwables.propagate(ex);
        }

        TaskReport report = Exec.newTaskReport();
        if (task.getIncrementalColumn() != null) {
            report.set("last_record", task.getLastRecord().get());
        }
        return report;
    }

    protected ColumnGetterFactory newColumnGetterFactory(PageBuilder pageBuilder, DateTimeZone dateTimeZone)
    {
        return new ColumnGetterFactory(pageBuilder, dateTimeZone);
    }

    private List<ColumnGetter> newColumnGetters(PluginTask task, JdbcSchema querySchema, PageBuilder pageBuilder)
            throws SQLException
    {
        ColumnGetterFactory factory = newColumnGetterFactory(pageBuilder, task.getDefaultTimeZone());
        ImmutableList.Builder<ColumnGetter> getters = ImmutableList.builder();
        for (JdbcColumn c : querySchema.getColumns()) {
            JdbcColumnOption columnOption = columnOptionOf(task.getColumnOptions(), task.getDefaultColumnOptions(), c, factory.getJdbcType(c.getSqlType()));
            getters.add(factory.newColumnGetter(c, columnOption));
        }
        return getters.build();
    }

    private static JdbcColumnOption columnOptionOf(Map<String, JdbcColumnOption> columnOptions, Map<String, JdbcColumnOption> defaultColumnOptions, JdbcColumn targetColumn, String targetColumnSQLType)
    {
        JdbcColumnOption columnOption = columnOptions.get(targetColumn.getName());
        if (columnOption == null) {
            String foundName = null;
            for (Map.Entry<String, JdbcColumnOption> entry : columnOptions.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(targetColumn.getName())) {
                    if (columnOption != null) {
                        throw new ConfigException(String.format("Cannot specify column '%s' because both '%s' and '%s' exist in column_options.",
                                targetColumn.getName(), foundName, entry.getKey()));
                    }
                    foundName = entry.getKey();
                    columnOption = entry.getValue();
                }
            }
        }

        return Optional
                .fromNullable(columnOption)
                .or(Optional.fromNullable(defaultColumnOptions.get(targetColumnSQLType)))
                .or(
                    // default column option
                    new Supplier<JdbcColumnOption>()
                    {
                        public JdbcColumnOption get()
                        {
                            return Exec.newConfigSource().loadConfig(JdbcColumnOption.class);
                        }
                    });
    }

    private boolean fetch(BatchSelect cursor,
            List<ColumnGetter> getters, PageBuilder pageBuilder, PluginTask task) throws SQLException
    {
        ResultSet result = cursor.fetch();
        if (result == null || !result.next()) {
            return false;
        }

        Map<String, String> temporaryLastRecord = new HashMap<>();
        Map<String, String> lastRecord = new HashMap<>();

        List<Column> columns = pageBuilder.getSchema().getColumns();
        long rows = 0;
        long reportRows = 500;
        do {
            temporaryLastRecord.clear();
            for (int i=0; i < getters.size(); i++) {
                int index = i + 1;  // JDBC column index begins from 1
                getters.get(i).getAndSet(result, index, columns.get(i));
                String k = columns.get(i).getName();
                temporaryLastRecord.put(k, result.getString(index));
            }
            pageBuilder.addRecord();
            rows++;
            if (rows % reportRows == 0) {
                logger.info(String.format("Fetched %,d rows.", rows));
                reportRows *= 2;
            }
        } while (result.next());

        if (task.getIncrementalColumn().isPresent()) {
            for (String key : task.getIncrementalColumn().get()) {
                if (temporaryLastRecord.containsKey(key)) {
                    lastRecord.put(key, temporaryLastRecord.get(key));
                }
            }
            task.setLastRecord(Optional.of(lastRecord));
        }

        return true;
    }

    //// TODO move to embulk.spi.util?
    //private static class ListPageOutput
    //{
    //    public ImmutableList.Builder<Page> pages;
    //
    //    public ListPageOutput()
    //    {
    //        reset();
    //    }
    //
    //    @Override
    //    public void add(Page page)
    //    {
    //        pages.add(page);
    //    }
    //
    //    @Override
    //    public void finish()
    //    {
    //    }
    //
    //    @Override
    //    public void close()
    //    {
    //    }
    //
    //    public List<Page> getPages()
    //    {
    //        return pages.build();
    //    }
    //
    //    public void reset()
    //    {
    //        pages = ImmutableList.builder();
    //    }
    //}

    protected void loadDriverJar(String glob)
    {
        // TODO match glob
        PluginClassLoader loader = (PluginClassLoader) getClass().getClassLoader();
        loader.addPath(Paths.get(glob));
    }
}
