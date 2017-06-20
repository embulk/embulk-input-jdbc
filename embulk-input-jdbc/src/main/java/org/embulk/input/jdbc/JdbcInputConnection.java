package org.embulk.input.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Set;

import org.embulk.config.ConfigException;
import org.embulk.spi.Exec;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.slf4j.Logger;

import java.util.List;
import java.util.ArrayList;
import static java.util.Locale.ENGLISH;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class JdbcInputConnection
        implements AutoCloseable
{
    protected final Logger logger = Exec.getLogger(getClass());

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
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        try {
            while(rs.next()) {
                builder.add(rs.getString("COLUMN_NAME"));
            }
        } finally {
            rs.close();
        }
        return builder.build();
    }

    protected JdbcSchema getSchemaOfResultMetadata(ResultSetMetaData metadata) throws SQLException
    {
        ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        for (int i=0; i < metadata.getColumnCount(); i++) {
            int index = i + 1;  // JDBC column index begins from 1
            String name = metadata.getColumnLabel(index);
            String typeName = metadata.getColumnTypeName(index);
            int sqlType = metadata.getColumnType(index);
            int scale = metadata.getScale(index);
            int precision = metadata.getPrecision(index);
            columns.add(new JdbcColumn(name, typeName, sqlType, precision, scale));
        }
        return new JdbcSchema(columns.build());
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
        sb.append(selectExpression.or("*"));
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
            List<Integer> incrementalColumnIndexes, List<JsonNode> incrementalValues) throws SQLException
    {
        List<JdbcLiteral> parameters = ImmutableList.of();

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
                    querySchema, incrementalColumnIndexes, incrementalValues);
            sb.append(")");

            newWhereCondition = Optional.of(sb.toString());
        }
        else {
            newWhereCondition = whereCondition;
        }

        Optional<String> newOrderByExpression;
        {
            StringBuilder sb = new StringBuilder();
            buildIncrementalOrderTo(sb, querySchema, incrementalColumnIndexes);
            newOrderByExpression = Optional.of(sb.toString());
        }

        String newQuery = buildSelectQuery(
                tableName, selectExpression, newWhereCondition,
                newOrderByExpression);

        return new PreparedQuery(newQuery, parameters);
    }

    public PreparedQuery wrapIncrementalQuery(String rawQuery, JdbcSchema querySchema,
            List<Integer> incrementalColumnIndexes, List<JsonNode> incrementalValues) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        List<JdbcLiteral> parameters = ImmutableList.of();

        sb.append("SELECT * FROM (");
        sb.append(truncateStatementDelimiter(rawQuery));
        sb.append(") embulk_incremental_");

        if (incrementalValues != null) {
            sb.append(" WHERE ");
            parameters = buildIncrementalConditionTo(sb,
                    querySchema, incrementalColumnIndexes, incrementalValues);
        }

        sb.append(" ORDER BY ");
        buildIncrementalOrderTo(sb, querySchema, incrementalColumnIndexes);

        return new PreparedQuery(sb.toString(), parameters);
    }

    private List<JdbcLiteral> buildIncrementalConditionTo(
            StringBuilder sb,
            JdbcSchema querySchema,
            List<Integer> incrementalColumnIndexes, List<JsonNode> incrementalValues) throws SQLException
    {
        ImmutableList.Builder<JdbcLiteral> parameters = ImmutableList.builder();

        List<String> leftColumnNames = new ArrayList<>();
        List<JdbcLiteral> rightLiterals = new ArrayList<>();
        for (int n = 0; n < incrementalColumnIndexes.size(); n++) {
            int columnIndex = incrementalColumnIndexes.get(n);
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

        return parameters.build();
    }

    private void buildIncrementalOrderTo(StringBuilder sb,
            JdbcSchema querySchema, List<Integer> incrementalColumnIndexes)
    {
        boolean first = true;
        for (int i : incrementalColumnIndexes) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(quoteIdentifierString(querySchema.getColumnName(i)));
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
        ImmutableSet.Builder<String> columnNamesBuilder = ImmutableSet.builder();
        try (ResultSet rs = connection.getMetaData().getColumns(null, schemaName, tableName, null)) {
            while (rs.next()) {
                columnNamesBuilder.add(rs.getString("COLUMN_NAME"));
            }
            return columnNamesBuilder.build();
        }
    }

    public void showDriverVersion() throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        logger.info(String.format(Locale.ENGLISH,"Using JDBC Driver %s",meta.getDriverVersion()));
    }
}
