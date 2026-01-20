package org.embulk.input.jdbc;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Optional;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import org.embulk.config.ConfigException;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
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
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.ZoneIdModule;

import static java.util.Locale.ENGLISH;

public abstract class AbstractJdbcInputPlugin
        implements InputPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractJdbcInputPlugin.class);

    protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().addModule(ZoneIdModule.withLegacyNames()).build();

    protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    protected static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

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

        @Config("use_raw_query_with_incremental")
        @ConfigDefault("false")
        public boolean getUseRawQueryWithIncremental();

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
        public ZoneId getDefaultTimeZone();

        @Config("default_column_options")
        @ConfigDefault("{}")
        public Map<String, JdbcColumnOption> getDefaultColumnOptions();

        @Config("before_setup")
        @ConfigDefault("null")
        public Optional<String> getBeforeSetup();

        @Config("before_select")
        @ConfigDefault("null")
        public Optional<String> getBeforeSelect();

        @Config("after_select")
        @ConfigDefault("null")
        public Optional<String> getAfterSelect();

        public PreparedQuery getBuiltQuery();
        public void setBuiltQuery(PreparedQuery query);

        public JdbcSchema getQuerySchema();
        public void setQuerySchema(JdbcSchema schema);

        public List<Integer> getIncrementalColumnIndexes();
        public void setIncrementalColumnIndexes(List<Integer> indexes);
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
        final PluginTask task = CONFIG_MAPPER.map(config, this.getTaskClass());

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

            if (task.getBeforeSetup().isPresent()) {
                con.executeUpdate(task.getBeforeSetup().get());
                con.commit();
            }

            // TODO incremental_columns is not set => get primary key
            schema = setupTask(con, task);
        } catch (SQLException ex) {
            if (ex.getCause() instanceof UnknownHostException) {
                throw new ConfigException(ex);
            }
            throw new RuntimeException(ex);
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

        JdbcSchema querySchema = null;
        if (task.getUseRawQueryWithIncremental()) {
            String temporaryQuery = rawQuery;

            // Insert pair of columnName:columnIndex order by column name length DESC
            TreeMap<String, Integer> columnNames = con.createColumnNameSortedMap();
            for (int i = 0; i < task.getIncrementalColumns().size(); i++) {
                columnNames.put(task.getIncrementalColumns().get(i), i);
            }

            for (Map.Entry<String, Integer> column : columnNames.entrySet()) {
                // Temporary replace place holder like ":id" with "?" to avoid SyntaxException while getting schema.
                temporaryQuery = temporaryQuery.replace(":" + column.getKey(), "?");
            }
            querySchema = con.getSchemaOfQuery(temporaryQuery);
        } else {
            querySchema = con.getSchemaOfQuery(rawQuery);
        }
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
                preparedQuery = con.wrapIncrementalQuery(rawQuery, querySchema, incrementalColumns, lastRecord, task.getUseRawQueryWithIncremental());
            }
            else {
                preparedQuery = con.rebuildIncrementalQuery(
                        task.getTable().get(), task.getSelect(),
                        task.getWhere(),
                        querySchema, incrementalColumns, lastRecord);
            }
        }
        else {
            task.setIncrementalColumnIndexes(Collections.<Integer>emptyList());
            preparedQuery = new PreparedQuery(rawQuery, Collections.<JdbcLiteral>emptyList());
        }

        task.setBuiltQuery(preparedQuery);

        // validate column_options
        newColumnGetters(con, task, querySchema, null);

        ColumnGetterFactory factory = newColumnGetterFactory(null, task.getDefaultTimeZone());
        final ArrayList<Column> columns = new ArrayList<>();
        for (int i = 0; i < querySchema.getCount(); i++) {
            JdbcColumn column = querySchema.getColumn(i);
            JdbcColumnOption columnOption = columnOptionOf(task.getColumnOptions(), task.getDefaultColumnOptions(), column, factory.getJdbcType(column.getSqlType()));
            columns.add(new Column(i,
                    column.getName(),
                    factory.newColumnGetter(con, task, column, columnOption).getToType()));
        }
        return new Schema(Collections.unmodifiableList(columns));
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
        final ArrayList<Integer> indices = new ArrayList<>();
        for (String name : incrementalColumns) {
            Optional<Integer> index = schema.findColumn(name);
            if (index.isPresent()) {
                indices.add(index.get());
            }
            else {
                throw new ConfigException(String.format(ENGLISH,
                        "Column name '%s' is in incremental_columns option does not exist",
                        name));
            }
        }
        return Collections.unmodifiableList(indices);
    }

    private String getRawQuery(PluginTask task, JdbcInputConnection con) throws SQLException
    {
        if (task.getQuery().isPresent()) {
            if (task.getTable().isPresent() || task.getSelect().isPresent() ||
                    task.getWhere().isPresent() || task.getOrderBy().isPresent()) {
                throw new ConfigException("'table', 'select', 'where' and 'order_by' parameters are unnecessary if 'query' parameter is set.");
            } else if (task.getUseRawQueryWithIncremental()) {
                String rawQuery = task.getQuery().get();
                for (String columnName : task.getIncrementalColumns()) {
                    if (!rawQuery.contains(":" + columnName)) {
                        throw new ConfigException(String.format("Column \":%s\" doesn't exist in query string", columnName));
                    }
                }
                if (!task.getLastRecord().isPresent()) {
                    throw new ConfigException("'last_record' is required when 'use_raw_query_with_incremental' is set to true");
                }
                if (task.getLastRecord().get().size() != task.getIncrementalColumns().size()) {
                    throw new ConfigException("size of 'last_record' is different from of 'incremental_columns'");
                }
            } else if (!task.getUseRawQueryWithIncremental() && (!task.getIncrementalColumns().isEmpty() || task.getLastRecord().isPresent())) {
                throw new ConfigException("'incremental_columns' and 'last_record' parameters are not supported if 'query' parameter is set and 'use_raw_query_with_incremental' is set to false.");
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
        final PluginTask task = TASK_MAPPER.map(taskSource, this.getTaskClass());

        // TODO when parallel execution is implemented and enabled, (maybe) order_by
        //      is necessary to resume. transaction() gets the range of order_by
        //      colum and set it to WHERE condition to make the operation deterministic

        return buildNextConfigDiff(task, control.run(taskSource, schema, taskCount));
    }

    public ConfigDiff guess(ConfigSource config)
    {
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    protected ConfigDiff buildNextConfigDiff(PluginTask task, List<TaskReport> reports)
    {
        final ConfigDiff next = CONFIG_MAPPER_FACTORY.newConfigDiff();
        if (reports.size() > 0 && reports.get(0).has("last_record")) {
            // |reports| are from embulk-core, then their backend is Jackson on the embulk-core side.
            // To render |JsonNode| (that is on the plugin side) from |reports|, they need to be rebuilt.
            final TaskReport report = CONFIG_MAPPER_FACTORY.rebuildTaskReport(reports.get(0));
            next.set("last_record", report.get(JsonNode.class, "last_record"));
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
            final ArrayList<JsonNode> values = new ArrayList<>();
            for (int i = 0; i < lastValues.length; i++) {
                if (lastValues[i] == null || lastValues[i].isNull()) {
                    throw new DataException(String.format(ENGLISH,
                            "incremental_columns can't include null values but the last row is null at column '%s'",
                            columnNames.get(i)));
                }
                values.add(lastValues[i]);
            }
            return Collections.unmodifiableList(values);
        }
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        final PluginTask task = TASK_MAPPER.map(taskSource, this.getTaskClass());

        PreparedQuery builtQuery = task.getBuiltQuery();
        JdbcSchema querySchema = task.getQuerySchema();
        BufferAllocator allocator = Exec.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        long totalRows = 0;

        LastRecordStore lastRecordStore = null;

        try (JdbcInputConnection con = newConnection(task)) {
            if (task.getBeforeSelect().isPresent()) {
                con.executeUpdate(task.getBeforeSelect().get());
            }

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
            pageBuilder.close();

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
            }
            con.commit();

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        final TaskReport report = CONFIG_MAPPER_FACTORY.newTaskReport();
        if (lastRecordStore != null) {
            report.set("last_record", lastRecordStore.getList());
        }

        return report;
    }

    protected ColumnGetterFactory newColumnGetterFactory(PageBuilder pageBuilder, ZoneId dateTimeZone)
    {
        return new ColumnGetterFactory(pageBuilder, dateTimeZone);
    }

    private List<ColumnGetter> newColumnGetters(JdbcInputConnection con, PluginTask task, JdbcSchema querySchema, PageBuilder pageBuilder)
            throws SQLException
    {
        ColumnGetterFactory factory = newColumnGetterFactory(pageBuilder, task.getDefaultTimeZone());
        final ArrayList<ColumnGetter> getters = new ArrayList<>();
        for (JdbcColumn c : querySchema.getColumns()) {
            JdbcColumnOption columnOption = columnOptionOf(task.getColumnOptions(), task.getDefaultColumnOptions(), c, factory.getJdbcType(c.getSqlType()));
            getters.add(factory.newColumnGetter(con, task, c, columnOption));
        }
        return Collections.unmodifiableList(getters);
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

        if (columnOption != null) {
            return columnOption;
        }
        final JdbcColumnOption defaultColumnOption = defaultColumnOptions.get(targetColumnSQLType);
        if (defaultColumnOption != null) {
            return defaultColumnOption;
        }
        return CONFIG_MAPPER.map(CONFIG_MAPPER_FACTORY.newConfigSource(), JdbcColumnOption.class);
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
    //    public ArrayList<Page> pages;
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
    //        pages = Collections.unmodifiableList(builder);
    //    }
    //}

    protected void addDriverJarToClasspath(String glob)
    {
        // TODO match glob
        final ClassLoader loader = getClass().getClassLoader();
        if (!(loader instanceof URLClassLoader)) {
            throw new RuntimeException("Plugin is not loaded by URLClassLoader unexpectedly.");
        }
        if (!"org.embulk.plugin.PluginClassLoader".equals(loader.getClass().getName())) {
            throw new RuntimeException("Plugin is not loaded by PluginClassLoader unexpectedly.");
        }
        Path path = Paths.get(glob);
        if (!path.toFile().exists()) {
             throw new ConfigException("The specified driver jar doesn't exist: " + glob);
        }
        final Method addPathMethod;
        try {
            addPathMethod = loader.getClass().getMethod("addPath", Path.class);
        } catch (final NoSuchMethodException ex) {
            throw new RuntimeException("Plugin is not loaded a ClassLoader which has addPath(Path), unexpectedly.");
        }
        try {
            addPathMethod.invoke(loader, Paths.get(glob));
        } catch (final IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch (final InvocationTargetException ex) {
            final Throwable targetException = ex.getTargetException();
            if (targetException instanceof MalformedURLException) {
                throw new IllegalArgumentException(targetException);
            } else if (targetException instanceof RuntimeException) {
                throw (RuntimeException) targetException;
            } else {
                throw new RuntimeException(targetException);
            }
        }
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
