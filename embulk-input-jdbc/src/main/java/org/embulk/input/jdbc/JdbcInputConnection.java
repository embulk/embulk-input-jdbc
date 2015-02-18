package org.embulk.input.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.embulk.spi.Exec;

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

    protected String buildSelectQuery(String tableName,
            Optional<String> selectColumnList, Optional<String> whereCondition,
            Optional<String> orderByColumn)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        sb.append(selectColumnList.or("*"));
        sb.append(" FROM ").append(quoteIdentifierString(tableName));
        if (whereCondition.isPresent()) {
            sb.append(" WHERE ").append(whereCondition.get());
        }
        if (orderByColumn.isPresent()) {
            sb.append("ORDER BY ").append(quoteIdentifierString(orderByColumn.get())).append(" ASC");
        }

        return sb.toString();
    }

    public JdbcSchema getSchemaOfQuery(String tableName,
            Optional<String> selectColumnList, Optional<String> whereCondition,
            Optional<String> orderByColumn) throws SQLException
    {
        String query = buildSelectQuery(tableName, selectColumnList, whereCondition,
                orderByColumn);
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
            String name = metadata.getColumnName(index);
            String typeName = metadata.getColumnTypeName(index);
            int sqlType = metadata.getColumnType(index);
            //String scale = metadata.getScale(index)
            //String precision = metadata.getPrecision(index)
            columns.add(new JdbcColumn(name, typeName, sqlType));
        }
        return new JdbcSchema(columns.build());
    }

    public BatchSelect newSelectCursor(String tableName,
            Optional<String> selectColumnList, Optional<String> whereCondition,
            Optional<String> orderByColumn, int fetchRows) throws SQLException
    {
        String select = buildSelectQuery(tableName, selectColumnList, whereCondition, orderByColumn);
        return newBatchSelect(select, fetchRows);
    }

    protected BatchSelect newBatchSelect(String select, int fetchRows) throws SQLException
    {
        logger.info("SQL: " + select);
        PreparedStatement stmt = connection.prepareStatement(select);
        stmt.setFetchSize(fetchRows);
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

        public SingleSelect(PreparedStatement fetchStatement) throws SQLException
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
}
