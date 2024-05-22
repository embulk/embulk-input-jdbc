package org.embulk.input.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.embulk.input.jdbc.getter.ColumnGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public class JdbcInputConnection
        implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(JdbcInputConnection.class);

    protected final Connection connection;
    protected final String schemaName;
    protected final DatabaseMetaData databaseMetaData;
    protected String identifierQuoteString;

    public JdbcInputConnection(Connection connection, String schemaName)
            throws SQLException
    {
        this.connection = connection;
        this.schemaName = schemaName;
        this.databaseMetaData = connection.getMetaData();
        this.identifierQuoteString = databaseMetaData.getIdentifierQuoteString();
        if (schemaName != null) {
            setSearchPath(schemaName);
        }
        connection.setAutoCommit(false);
    }

    protected void setSearchPath(String schema) throws SQLException
    {
        String sql = "SET search_path TO " + quoteIdentifierString(schema);
        executeUpdate(sql);
    }

    public JdbcSchema getSchemaOfQuery(String query) throws SQLException
    {
        PreparedStatement stmt = connection.prepareStatement(query);
        try {
            return getSchemaOfResultMetadata(stmt.getMetaData());
        } finally {
            stmt.close();
        }
    }

    public List<String> getPrimaryKeys(String tableName) throws SQLException
    {
        ResultSet rs = databaseMetaData.getPrimaryKeys(null, schemaName, tableName);
        final ArrayList<String> primaryKeys = new ArrayList<>();
        try {
            while(rs.next()) {
                primaryKeys.add(rs.getString("COLUMN_NAME"));
            }
        } finally {
            rs.close();
        }
        return Collections.unmodifiableList(primaryKeys);
    }

    protected JdbcSchema getSchemaOfResultMetadata(ResultSetMetaData metadata) throws SQLException
    {
        final ArrayList<JdbcColumn> columns = new ArrayList<>();
        for (int i=0; i < metadata.getColumnCount(); i++) {
            int index = i + 1;  // JDBC column index begins from 1
            String name = metadata.getColumnLabel(index);
            String typeName = metadata.getColumnTypeName(index);
            int sqlType = metadata.getColumnType(index);
            int scale = metadata.getScale(index);
            int precision = metadata.getPrecision(index);
            columns.add(new JdbcColumn(name, typeName, sqlType, precision, scale));
        }
        return new JdbcSchema(Collections.unmodifiableList(columns));
    }

    public static class PreparedQuery
    {
        private final String query;
        private final List<JdbcLiteral> parameters;

        @JsonCreator
        public PreparedQuery(
                @JsonProperty("query") String query,
                @JsonProperty("parameters") List<JdbcLiteral> parameters)
        {
            this.query = query;
            this.parameters = parameters;
        }

        @JsonProperty("query")
        public String getQuery()
        {
            return query;
        }

        @JsonProperty("parameters")
        public List<JdbcLiteral> getParameters()
        {
            return parameters;
        }
    }

    public BatchSelect newSelectCursor(PreparedQuery preparedQuery,
            List<ColumnGetter> getters,
            int fetchRows, int queryTimeout) throws SQLException
    {
        return newBatchSelect(preparedQuery, getters, fetchRows, queryTimeout);
    }

    protected BatchSelect newBatchSelect(PreparedQuery preparedQuery,
            List<ColumnGetter> getters,
            int fetchRows, int queryTimeout) throws SQLException
    {
        String query = preparedQuery.getQuery();
        List<JdbcLiteral> params = preparedQuery.getParameters();

        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setFetchSize(fetchRows);
        stmt.setQueryTimeout(queryTimeout);
        logger.info("SQL: " + query);
        if (!params.isEmpty()) {
            logger.info("Parameters: {}", params);
            prepareParameters(stmt, getters, params);
        }
        return new SingleSelect(stmt);
    }

    protected void prepareParameters(PreparedStatement stmt, List<ColumnGetter> getters,
            List<JdbcLiteral> parameters)
        throws SQLException
    {
        for (int i = 0; i < parameters.size(); i++) {
            JdbcLiteral literal = parameters.get(i);
            ColumnGetter getter = getters.get(literal.getColumnIndex());
            int index = i + 1;  // JDBC column index begins from 1
            getter.decodeFromJsonTo(stmt, index, literal.getValue());
        }
    }

    public interface BatchSelect
            extends AutoCloseable
    {
        public ResultSet fetch() throws SQLException;

        @Override
        public void close() throws SQLException;
    }

    public class SingleSelect
            implements BatchSelect
    {
        private final PreparedStatement fetchStatement;
        private boolean fetched = false;

        public SingleSelect(PreparedStatement fetchStatement)
        {
            this.fetchStatement = fetchStatement;
        }

        public ResultSet fetch() throws SQLException
        {
            if (fetched == true) {
                return null;
            }

            long startTime = System.currentTimeMillis();

            ResultSet rs = fetchStatement.executeQuery();

            double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
            logger.info(String.format("> %.2f seconds", seconds));
            fetched = true;
            return rs;
        }

        public void close() throws SQLException
        {
            // TODO close?
        }
    }

    @Override
    public void close() throws SQLException
    {
        // This is for the JDBC driver that requires an explicit commit or rollback before closing the connection (e.g., DB2).
        connection.rollback();

        connection.close();
    }

    protected void executeUpdate(String sql) throws SQLException
    {
        logger.info("SQL: " + sql);
        Statement stmt = connection.createStatement();
        try {
            stmt.executeUpdate(sql);
        } finally {
            stmt.close();
        }
    }

    // TODO share code with embulk-output-jdbc
    protected String quoteIdentifierString(String str)
    {
        return identifierQuoteString + str + identifierQuoteString;
    }

    protected String buildTableName(String tableName)
    {
        return quoteIdentifierString(tableName);
    }

    public String buildSelectQuery(String tableName,
            Optional<String> selectExpression, Optional<String> whereCondition,
            Optional<String> orderByExpression) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        sb.append(selectExpression.orElse("*"));
        sb.append(" FROM ").append(buildTableName(tableName));

        if (whereCondition.isPresent()) {
            sb.append(" WHERE ").append(whereCondition.get());
        }

        if (orderByExpression.isPresent()) {
            sb.append(" ORDER BY ").append(orderByExpression.get());
        }

        return sb.toString();
    }

    public PreparedQuery rebuildIncrementalQuery(String tableName,
            Optional<String> selectExpression, Optional<String> whereCondition,
            JdbcSchema querySchema,
            List<String> incrementalColumns, List<JsonNode> incrementalValues) throws SQLException
    {
        List<JdbcLiteral> parameters = Collections.emptyList();

        Optional<String> newWhereCondition;
        if (incrementalValues != null) {
            StringBuilder sb = new StringBuilder();

            if (whereCondition.isPresent()) {
                sb.append("(");
                sb.append(whereCondition.get());
                sb.append(") AND ");
            }

            sb.append("(");
            parameters = buildIncrementalConditionTo(sb,
                    querySchema, incrementalColumns, incrementalValues);
            sb.append(")");

            newWhereCondition = Optional.of(sb.toString());
        }
        else {
            newWhereCondition = whereCondition;
        }

        Optional<String> newOrderByExpression;
        {
            StringBuilder sb = new StringBuilder();
            buildIncrementalOrderTo(sb, querySchema, incrementalColumns);
            newOrderByExpression = Optional.of(sb.toString());
        }

        String newQuery = buildSelectQuery(
                tableName, selectExpression, newWhereCondition,
                newOrderByExpression);

        return new PreparedQuery(newQuery, parameters);
    }

    public PreparedQuery wrapIncrementalQuery(String rawQuery, JdbcSchema querySchema,
            List<String> incrementalColumns, List<JsonNode> incrementalValues,
                                              boolean useRawQuery) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        List<JdbcLiteral> parameters = Collections.emptyList();

        if (useRawQuery) {
            parameters = replacePlaceholder(sb, rawQuery, querySchema, incrementalColumns, incrementalValues);
        } else {
            sb.append("SELECT * FROM (");
            sb.append(truncateStatementDelimiter(rawQuery));
            sb.append(") embulk_incremental_");

            if (incrementalValues != null) {
                sb.append(" WHERE ");
                parameters = buildIncrementalConditionTo(sb,
                        querySchema, incrementalColumns, incrementalValues);
            }

            sb.append(" ORDER BY ");
            buildIncrementalOrderTo(sb, querySchema, incrementalColumns);
        }

        return new PreparedQuery(sb.toString(), parameters);
    }

    private List<JdbcLiteral> buildIncrementalConditionTo(
            StringBuilder sb,
            JdbcSchema querySchema,
            List<String> incrementalColumns, List<JsonNode> incrementalValues) throws SQLException
    {
        final ArrayList<JdbcLiteral> parameters = new ArrayList<>();

        List<String> leftColumnNames = new ArrayList<>();
        List<JdbcLiteral> rightLiterals = new ArrayList<>();
        for (int n = 0; n < incrementalColumns.size(); n++) {
            int columnIndex = findIncrementalColumnIndex(querySchema, incrementalColumns.get(n));
            JsonNode value = incrementalValues.get(n);
            leftColumnNames.add(querySchema.getColumnName(columnIndex));
            rightLiterals.add(new JdbcLiteral(columnIndex, value));
        }

        for (int n = 0; n < leftColumnNames.size(); n++) {
            if (n > 0) {
                sb.append(" OR ");
            }
            sb.append("(");

            for (int i = 0; i < n; i++) {
                sb.append(quoteIdentifierString(leftColumnNames.get(i)));
                sb.append(" = ?");
                parameters.add(rightLiterals.get(i));
                sb.append(" AND ");
            }
            sb.append(quoteIdentifierString(leftColumnNames.get(n)));
            sb.append(" > ?");
            parameters.add(rightLiterals.get(n));

            sb.append(")");
        }

        return Collections.unmodifiableList(parameters);
    }

    private int findIncrementalColumnIndex(JdbcSchema schema, String incrementalColumn)
            throws SQLException
    {
        Optional<Integer> index = schema.findColumn(incrementalColumn);
        // must be present because already checked in AbstractJdbcInputPlugin.findIncrementalColumnIndexes .
        return index.get().intValue();
    }

    protected TreeMap<String, Integer> createColumnNameSortedMap()
    {
        // sort column names in descending order of the length
        return new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String val1, String val2) {
                int c = val2.length() - val1.length();
                if (c != 0) {
                    return c;
                }
                return val1.compareTo(val2);
            }
        });
    }

    private List<JdbcLiteral> replacePlaceholder(StringBuilder sb, String rawQuery, JdbcSchema querySchema,
                                                 List<String> incrementalColumns, List<JsonNode> incrementalValues)
            throws SQLException
    {
        // Insert pair of columnName:columnIndex order by column name length DESC
        TreeMap<String, Integer> columnNames = createColumnNameSortedMap();

        final ArrayList<JdbcLiteral> parameters = new ArrayList<>();
        for (String columnName : incrementalColumns) {
            int columnIndex = findIncrementalColumnIndex(querySchema, columnName);
            columnNames.put(columnName, columnIndex);
        }

        // Add value of each columns
        for (Map.Entry<Integer, Integer> columnPosition: generateColumnPositionList(rawQuery, columnNames).entrySet()) {
            int columnIndex = columnPosition.getValue();
            JsonNode value = incrementalValues.get(columnIndex);
            parameters.add(new JdbcLiteral(columnIndex, value));
        }

        // Replace placeholder ":column1" string with "?"
        for (Entry<String, Integer> column : columnNames.entrySet()) {
            String columnName = column.getKey();
            while (rawQuery.contains(":" + columnName)) {
                rawQuery = rawQuery.replaceFirst(":" + columnName, "?");
            }
        }

        sb.append(rawQuery);

        return Collections.unmodifiableList(parameters);
    }

    /*
    * This method parse original query that contains placeholder ":column" and store its index position and columnIndex value in Map
    *
    * @param query string that contains placeholder like ":column"
    * @param pair of columnName:columnIndex sorted by column name length desc ["num2", 1]["num", 0]
    * @return pair of index position where ":column" appears and columnIndex sorted by index position [65,0][105,0][121,1]
    *
    * last_record: [1,101]
    * SELECT * FROM query_load WHERE
    *   num IS NOT NULL
    *   AND num > :num
    *   AND num2 IS NOT NULL
    *   OR (num = :num AND num2 > :num2)
    * ORDER BY num ASC, num2 ASC
    * in above case, return value will be [65,0][105,0][121,1]
    */
    private TreeMap<Integer, Integer> generateColumnPositionList(String rawQuery, TreeMap<String, Integer> columnNames)
    {
        TreeMap<Integer, Integer> columnPositionList = new TreeMap<>();

        for (Entry<String, Integer> column : columnNames.entrySet()) {
            int lastIndex = 0;
            while (true) {
                int index = rawQuery.indexOf(":" + column.getKey(), lastIndex);
                if (index == -1) {
                    break;
                }
                if (!columnPositionList.containsKey(index)) {
                    columnPositionList.put(index, column.getValue());
                }
                lastIndex = index + 2;
            }
        }
        return columnPositionList;
    }

    private void buildIncrementalOrderTo(StringBuilder sb,
            JdbcSchema querySchema, List<String> incrementalColumns) throws SQLException
    {
        boolean first = true;
        for (String incrementalColumn : incrementalColumns) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            int columnIndex = findIncrementalColumnIndex(querySchema, incrementalColumn);
            // the following column name is case sensitive,
            // so should use actual column name got by DatabaseMetaData.
            sb.append(quoteIdentifierString(querySchema.getColumnName(columnIndex)));
        }
    }

    protected String truncateStatementDelimiter(String rawQuery) throws SQLException
    {
        return rawQuery.replaceAll(";\\s*$", "");
    }

    public boolean tableExists(String tableName) throws SQLException
    {
        try (ResultSet rs = connection.getMetaData().getTables(null, schemaName, tableName, null)) {
            return rs.next();
        }
    }

    private Set<String> getColumnNames(String tableName) throws SQLException
    {
        final HashSet<String> columnNames = new HashSet<>();
        try (ResultSet rs = connection.getMetaData().getColumns(null, schemaName, tableName, null)) {
            while (rs.next()) {
                columnNames.add(rs.getString("COLUMN_NAME"));
            }
            return Collections.unmodifiableSet(columnNames);
        }
    }

    public void showDriverVersion() throws SQLException
    {
        DatabaseMetaData meta = connection.getMetaData();
        logger.info(String.format(Locale.ENGLISH,"Using JDBC Driver %s",meta.getDriverVersion()));
    }

    public void commit() throws SQLException
    {
        connection.commit();
    }
}
