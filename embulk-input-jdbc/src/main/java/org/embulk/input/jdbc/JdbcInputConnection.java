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
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

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

    public BatchSelect newSelectCursor(String query, int fetchRows, int queryTimeout) throws SQLException
    {
        return newBatchSelect(query, fetchRows, queryTimeout);
    }

    protected BatchSelect newBatchSelect(String query, int fetchRows, int queryTimeout) throws SQLException
    {
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setFetchSize(fetchRows);
        stmt.setQueryTimeout(queryTimeout);
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
            Optional<String> selectExpression, Optional<String> whereCondition, Optional<String> orderByExpression,
            List<String> incrementalColumns, Optional<Map<String, String>> lastRecord) throws SQLException
    {
        String actualTableName;
        if (tableExists(tableName)) {
            actualTableName = tableName;
        } else {
            String upperTableName = tableName.toUpperCase();
            String lowerTableName = tableName.toLowerCase();
            if (tableExists(upperTableName)) {
                if (tableExists(lowerTableName)) {
                    throw new ConfigException(String.format("Cannot specify table '%s' because both '%s' and '%s' exist.",
                            tableName, upperTableName, lowerTableName));
                } else {
                    actualTableName = upperTableName;
                }
            } else {
                if (tableExists(lowerTableName)) {
                    actualTableName = lowerTableName;
                } else {
                    actualTableName = tableName;
                }
            }
        }

        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        sb.append(selectExpression.or("*"));
        sb.append(" FROM ").append(buildTableName(actualTableName));

        if (whereCondition.isPresent() || incrementalColumns.isPresent()) {
            sb.append(" WHERE ");
            if (incrementalColumns.isPresent() && lastRecord.isPresent()) {
                for (int i = 0; i < incrementalColumns.get().size(); i++) {
                    String column = incrementalColumns.get().get(i);
                    sb.append(column).append( " > \"");
                    sb.append(lastRecord.get().get(column).replace("\"", "\\\""));
                    sb.append("\"");
                    if (i < incrementalColumns.get().size() - 1) {
                        sb.append(" AND ");
                    }
                }
            }
            if (whereCondition.isPresent()) {
                if (lastRecord.isPresent()) {
                    sb.append(" AND ");
                }
                sb.append(whereCondition.get());
            }
        }

        if (orderByColumn.isPresent() || !incrementalColumns.isEmpty()) {
            String actualOrderByColumn = null;
            if (orderByColumn.isPresent()) {
                Set<String> columnNames = getColumnNames(actualTableName);
                if (columnNames.contains(orderByColumn.get())) {
                    actualOrderByColumn = orderByColumn.get();
                } else {
                    String upperOrderByColumn = orderByColumn.get().toUpperCase();
                    String lowerOrderByColumn = orderByColumn.get().toLowerCase();
                    if (columnNames.contains(upperOrderByColumn)) {
                        if (columnNames.contains(lowerOrderByColumn)) {
                            throw new ConfigException(String.format("Cannot specify order-by colum '%s' because both '%s' and '%s' exist.",
                                    orderByColumn.get(), upperOrderByColumn, lowerOrderByColumn));
                        } else {
                            actualOrderByColumn = upperOrderByColumn;
                        }
                    } else {
                        if (columnNames.contains(lowerOrderByColumn)) {
                            actualOrderByColumn = lowerOrderByColumn;
                        } else {
                            actualOrderByColumn = orderByColumn.get();
                        }
                    }
                }
            }

            sb.append(" ORDER BY ");
            for (int i = 0; i < incrementalColumns.size(); i++) {
                String column = incrementalColumns.get(i);
                sb.append(quoteIdentifierString(column)).append(" ASC");
                if (i < incrementalColumns.size() - 1) {
                    sb.append(", ");
                }
            }
            if (orderByColumn.isPresent() && actualOrderByColumn != null) {
                if (lastRecord.isPresent()) {
                    sb.append(", ");
                }
                sb.append(quoteIdentifierString(actualOrderByColumn)).append(" ASC");
            }
        }
        if (orderByExpression.isPresent()) {
            sb.append("ORDER BY ").append(orderByExpression.get());
        }

        return sb.toString();
    }

    private boolean tableExists(String tableName) throws SQLException
    {
        try (ResultSet rs = connection.getMetaData().getTables(null, schemaName, tableName, null)) {
            return rs.next();
        }
    }

    private Set<String> getColumnNames(String tableName) throws SQLException
    {
        Builder<String> columnNamesBuilder = ImmutableSet.builder();
        try (ResultSet rs = connection.getMetaData().getColumns(null, schemaName, tableName, null)) {
            while (rs.next()) {
                columnNamesBuilder.add(rs.getString("COLUMN_NAME"));
            }
            return columnNamesBuilder.build();
        }
    }
}
