package org.embulk.input.jdbc;

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
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
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.Exec;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.jdbc.JdbcInputConnection.BatchSelect;
import org.embulk.input.jdbc.JdbcInputConnection.PreparedQuery;
import org.joda.time.DateTimeZone;

import static java.util.Locale.ENGLISH;

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
        public void setTable(Optional<String> normalizedTableName);

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

        @Config("incremental")
        @ConfigDefault("false")
        public boolean getIncremental();

        @Config("incremental_columns")
        @ConfigDefault("[]")
        public List<String> getIncrementalColumns();
        public void setIncrementalColumns(List<String> indexes);

        @Config("last_record")
        @ConfigDefault("null")
        public Optional<List<JsonNode>> getLastRecord();

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

        public PreparedQuery getBuiltQuery();
        public void setBuiltQuery(PreparedQuery query);

        public JdbcSchema getQuerySchema();
        public void setQuerySchema(JdbcSchema schema);

        public List<Integer> getIncrementalColumnIndexes();
        public void setIncrementalColumnIndexes(List<Integer> indexes);

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

        if (task.getIncremental()) {
            if (task.getOrderBy().isPresent()) {
                throw new ConfigException("order_by option must not be set if incremental is true");
            }
        }
        else {
            if (!task.getIncrementalColumns().isEmpty()) {
                throw new ConfigException("'incremental: true' must be set if incremental_columns is set");
            }
        }

        Schema schema;
        try (JdbcInputConnection con = newConnection(task)) {
            con.showDriverVersion();

            // TODO incremental_columns is not set => get primary key
            schema = setupTask(con, task);
        } catch (SQLException ex) {
            throw Throwables.propagate(ex);
        }

        return buildNextConfigDiff(task, control.run(task.dump(), schema, 1));
    }

    protected Schema setupTask(JdbcInputConnection con, PluginTask task) throws SQLException
    {
        if (task.getTable().isPresent()) {
            String actualTableName = normalizeTableNameCase(con, task.getTable().get());
            task.setTable(Optional.of(actualTableName));
        }

        // build SELECT query and gets schema of its result
        String rawQuery = getRawQuery(task, con);

        JdbcSchema querySchema = con.getSchemaOfQuery(rawQuery);
        task.setQuerySchema(querySchema);
        // query schema should not change after incremental query

        PreparedQuery preparedQuery;
        if (task.getIncremental()) {
            // build incremental query

            List<String> incrementalColumns = task.getIncrementalColumns();
            if (incrementalColumns.isEmpty()) {
                // incremental_columns is not set
                if (!task.getTable().isPresent()) {
                    throw new ConfigException("incremental_columns option must be set if incremental is true and custom query option is set");
                }
                // get primary keys from the target table to use them as incremental_columns
                List<String> primaryKeys = con.getPrimaryKeys(task.getTable().get());
                if (primaryKeys.isEmpty()) {
                    throw new ConfigException(String.format(ENGLISH,
                                "Primary key is not available at the table '%s'. incremental_columns option must be set",
                                task.getTable().get()));
                }
                logger.info("Using primary keys as incremental_columns: {}", primaryKeys);
                task.setIncrementalColumns(primaryKeys);
                incrementalColumns = primaryKeys;
            }

            List<Integer> incrementalColumnIndexes = findIncrementalColumnIndexes(querySchema, incrementalColumns);
            task.setIncrementalColumnIndexes(incrementalColumnIndexes);

            List<JsonNode> lastRecord;
            if (task.getLastRecord().isPresent()) {
                lastRecord = task.getLastRecord().get();
                if (lastRecord.size() != incrementalColumnIndexes.size()) {
                    throw new ConfigException("Number of values set at last_record must be same with number of columns set at incremental_columns");
                }
            }
            else {
                lastRecord = null;
            }

            if (task.getQuery().isPresent()) {
                preparedQuery = con.wrapIncrementalQuery(rawQuery, querySchema, incrementalColumnIndexes, lastRecord);
            }
            else {
                preparedQuery = con.rebuildIncrementalQuery(
                        task.getTable().get(), task.getSelect(),
                        task.getWhere(),
                        querySchema, incrementalColumnIndexes, lastRecord);
            }
        }
        else {
            task.setIncrementalColumnIndexes(ImmutableList.<Integer>of());
            preparedQuery = new PreparedQuery(rawQuery, ImmutableList.<JdbcLiteral>of());
        }

        task.setBuiltQuery(preparedQuery);

        // validate column_options
        newColumnGetters(con, task, querySchema, null);

        ColumnGetterFactory factory = newColumnGetterFactory(null, task.getDefaultTimeZone());
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (int i = 0; i < querySchema.getCount(); i++) {
            JdbcColumn column = querySchema.getColumn(i);
            JdbcColumnOption columnOption = columnOptionOf(task.getColumnOptions(), task.getDefaultColumnOptions(), column, factory.getJdbcType(column.getSqlType()));
            columns.add(new Column(i,
                    column.getName(),
                    factory.newColumnGetter(con, task, column, columnOption).getToType()));
        }
        return new Schema(columns.build());
    }

    private String normalizeTableNameCase(JdbcInputConnection con, String tableName)
        throws SQLException
    {
        if (con.tableExists(tableName)) {
            return tableName;
        } else {
            String upperTableName = tableName.toUpperCase();
            String lowerTableName = tableName.toLowerCase();
            boolean upperExists = con.tableExists(upperTableName);
            boolean lowerExists = con.tableExists(lowerTableName);
            if (upperExists && lowerExists) {
                    throw new ConfigException(String.format("Cannot specify table '%s' because both '%s' and '%s' exist.",
                            tableName, upperTableName, lowerTableName));
            } else if (upperExists) {
                return upperTableName;
            } else if (lowerExists) {
                return lowerTableName;
            } else {
                // fallback to the given table name. this may throw error later at getSchemaOfQuery
                return tableName;
            }
        }
    }

    private List<Integer> findIncrementalColumnIndexes(JdbcSchema schema, List<String> incrementalColumns)
        throws SQLException
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (String name : incrementalColumns) {
            Optional<Integer> index = schema.findColumn(name);
            if (index.isPresent()) {
                builder.add(index.get());
            }
            else {
                throw new ConfigException(String.format(ENGLISH,
                        "Column name '%s' is in incremental_columns option does not exist",
                        name));
            }
        }
        return builder.build();
    }

    private String getRawQuery(PluginTask task, JdbcInputConnection con) throws SQLException
    {
        if (task.getQuery().isPresent()) {
            if (task.getTable().isPresent() || task.getSelect().isPresent() ||
                    task.getWhere().isPresent() || task.getOrderBy().isPresent()) {
                throw new ConfigException("'table', 'select', 'where' and 'order_by' parameters are unnecessary if 'query' parameter is set.");
            } else if (!task.getIncrementalColumns().isEmpty() || task.getLastRecord().isPresent()) {
                throw new ConfigException("'incremental_columns' and 'last_record' parameters are not supported if 'query' parameter is set.");
            }
            return task.getQuery().get();
        } else if (task.getTable().isPresent()) {
            return con.buildSelectQuery(task.getTable().get(), task.getSelect(),
                    task.getWhere(), task.getOrderBy());
        } else {
            throw new ConfigException("'table' or 'query' parameter is required");
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
            next.set("last_record", reports.get(0).get(JsonNode.class, "last_record"));
        } else if (task.getLastRecord().isPresent()) {
            next.set("last_record", task.getLastRecord().get());
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

    private static class LastRecordStore
    {
        private final List<Integer> columnIndexes;
        private final JsonNode[] lastValues;
        private final List<String> columnNames;

        public LastRecordStore(List<Integer> columnIndexes, List<String> columnNames)
        {
            this.columnIndexes = columnIndexes;
            this.lastValues = new JsonNode[columnIndexes.size()];
            this.columnNames = columnNames;
        }

        public void accept(List<ColumnGetter> getters)
            throws SQLException
        {
            for (int i = 0; i < columnIndexes.size(); i++) {
                lastValues[i] = getters.get(columnIndexes.get(i)).encodeToJson();
            }
        }

        public List<JsonNode> getList()
        {
            ImmutableList.Builder<JsonNode> builder = ImmutableList.builder();
            for (int i = 0; i < lastValues.length; i++) {
                if (lastValues[i] == null || lastValues[i].isNull()) {
                    throw new DataException(String.format(ENGLISH,
                            "incremental_columns can't include null values but the last row is null at column '%s'",
                            columnNames.get(i)));
                }
                builder.add(lastValues[i]);
            }
            return builder.build();
        }
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(getTaskClass());

        PreparedQuery builtQuery = task.getBuiltQuery();
        JdbcSchema querySchema = task.getQuerySchema();
        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        long totalRows = 0;

        LastRecordStore lastRecordStore = null;

        try (JdbcInputConnection con = newConnection(task)) {
            List<ColumnGetter> getters = newColumnGetters(con, task, querySchema, pageBuilder);
            try (BatchSelect cursor = con.newSelectCursor(builtQuery, getters, task.getFetchRows(), task.getSocketTimeout())) {
                while (true) {
                    long rows = fetch(cursor, getters, pageBuilder);
                    if (rows <= 0L) {
                        break;
                    }
                    totalRows += rows;
                }
            }

            if (task.getIncremental() && totalRows > 0) {
                lastRecordStore = new LastRecordStore(task.getIncrementalColumnIndexes(), task.getIncrementalColumns());
                lastRecordStore.accept(getters);
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
        if (lastRecordStore != null) {
            report.set("last_record", lastRecordStore.getList());
        }

        return report;
    }

    protected ColumnGetterFactory newColumnGetterFactory(PageBuilder pageBuilder, DateTimeZone dateTimeZone)
    {
        return new ColumnGetterFactory(pageBuilder, dateTimeZone);
    }

    private List<ColumnGetter> newColumnGetters(JdbcInputConnection con, PluginTask task, JdbcSchema querySchema, PageBuilder pageBuilder)
            throws SQLException
    {
        ColumnGetterFactory factory = newColumnGetterFactory(pageBuilder, task.getDefaultTimeZone());
        ImmutableList.Builder<ColumnGetter> getters = ImmutableList.builder();
        for (JdbcColumn c : querySchema.getColumns()) {
            JdbcColumnOption columnOption = columnOptionOf(task.getColumnOptions(), task.getDefaultColumnOptions(), c, factory.getJdbcType(c.getSqlType()));
            getters.add(factory.newColumnGetter(con, task, c, columnOption));
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

    private long fetch(BatchSelect cursor,
            List<ColumnGetter> getters, PageBuilder pageBuilder) throws SQLException
    {
        ResultSet result = cursor.fetch();
        if (result == null || !result.next()) {
            return 0;
        }

        List<Column> columns = pageBuilder.getSchema().getColumns();
        long rows = 0;
        long reportRows = 500;
        do {
            for (int i=0; i < getters.size(); i++) {
                int index = i + 1;  // JDBC column index begins from 1
                getters.get(i).getAndSet(result, index, columns.get(i));
            }
            pageBuilder.addRecord();
            rows++;
            if (rows % reportRows == 0) {
                logger.info(String.format("Fetched %,d rows.", rows));
                reportRows *= 2;
            }
        } while (result.next());

        return rows;
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

    protected void loadDriver(String className, Optional<String> driverPath)
    {
        if (driverPath.isPresent()) {
            addDriverJarToClasspath(driverPath.get());
        } else {
            try {
                // Gradle test task will add JDBC driver to classpath
                Class.forName(className);

            } catch (ClassNotFoundException ex) {
                File root = findPluginRoot();
                File driverLib = new File(root, "default_jdbc_driver");
                File[] files = driverLib.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File file) {
                        return file.isFile() && file.getName().endsWith(".jar");
                    }
                });
                if (files == null || files.length == 0) {
                    throw new RuntimeException("Cannot find JDBC driver in '" + root.getAbsolutePath() + "'.");
                } else {
                    for (File file : files) {
                        logger.info("JDBC Driver = " + file.getAbsolutePath());
                        addDriverJarToClasspath(file.getAbsolutePath());
                    }
                }
            }
        }

        // Load JDBC Driver
        try {
            Class.forName(className);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void addDriverJarToClasspath(String glob)
    {
        // TODO match glob
        PluginClassLoader loader = (PluginClassLoader) getClass().getClassLoader();
        Path path = Paths.get(glob);
        if (!path.toFile().exists()) {
             throw new ConfigException("The specified driver jar doesn't exist: " + glob);
        }
        loader.addPath(Paths.get(glob));
    }

    protected File findPluginRoot()
    {
        try {
            URL url = getClass().getResource("/" + getClass().getName().replace('.', '/') + ".class");
            if (url.toString().startsWith("jar:")) {
                url = new URL(url.toString().replaceAll("^jar:", "").replaceAll("![^!]*$", ""));
            }

            File folder = new File(url.toURI()).getParentFile();
            for (;; folder = folder.getParentFile()) {
                if (folder == null) {
                    throw new RuntimeException("Cannot find 'embulk-input-xxx' folder.");
                }

                if (folder.getName().startsWith("embulk-input-")) {
                    return folder;
                }
            }
        } catch (MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    protected void logConnectionProperties(String url, Properties props)
    {
        Properties maskedProps = new Properties();
        for(String key : props.stringPropertyNames()) {
            if (key.equals("password")) {
                maskedProps.setProperty(key, "***");
            } else {
                maskedProps.setProperty(key, props.getProperty(key));
            }
        }
        logger.info("Connecting to {} options {}", url, maskedProps);
    }
}
