package org.embulk.input.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import org.embulk.config.ConfigException;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.util.List;
import static java.util.Locale.ENGLISH;

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

    public BatchSelect newSelectCursor(String query, List<Number> placeHolderValues,
            int fetchRows, int queryTimeout) throws SQLException
    {
        return newBatchSelect(query, placeHolderValues, fetchRows, queryTimeout);
    }

    protected BatchSelect newBatchSelect(String query, List<Number> placeHolderValues,
            int fetchRows, int queryTimeout) throws SQLException
    {
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setFetchSize(fetchRows);
        stmt.setQueryTimeout(queryTimeout);
        logger.info("SQL: " + query);
        if (!placeHolderValues.isEmpty()) {
            logger.info("Parameters: {}", placeHolderValues);
        }
        for (int i = 0; i < placeHolderValues.size(); i++) {
            stmt.setObject(i + 1, placeHolderValues.get(i));
        }
        return new SingleSelect(stmt);
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

    public String buildIncrementalQuery(String rawQuery, List<String> incrementalColumns,
            boolean generateIncrementalPlaceHolders) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT * FROM (");
        sb.append(truncateStatementDelimiter(rawQuery));
        sb.append(") embulk_incremental_");
        if (generateIncrementalPlaceHolders) {
            sb.append(" WHERE ");
            boolean first = true;
            for (String column : incrementalColumns) {
                if (first) {
                    first = false;
                } else {
                    sb.append(" AND ");
                }
                sb.append(quoteIdentifierString(column));
                sb.append(" > ?");
            }
        }
        sb.append(" ORDER BY ");

        boolean first = true;
        for (String column : incrementalColumns) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(quoteIdentifierString(column));
        }

        return sb.toString();
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
}
