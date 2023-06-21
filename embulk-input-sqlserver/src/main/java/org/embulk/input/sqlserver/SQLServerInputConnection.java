package org.embulk.input.sqlserver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.JdbcLiteral;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLServerInputConnection extends JdbcInputConnection
{
    private static final Logger logger = LoggerFactory.getLogger(SQLServerInputConnection.class);
    private String transactionIsolationLevel;

    public SQLServerInputConnection(Connection connection, String schemaName) throws SQLException
    {
        super(connection, schemaName);
    }

    public SQLServerInputConnection(Connection connection, String schemaName, String transactionIsolationLevel)
            throws SQLException
    {
        super(connection, schemaName);
        this.transactionIsolationLevel = transactionIsolationLevel;
    }

    @Override
    protected BatchSelect newBatchSelect(PreparedQuery preparedQuery, List<ColumnGetter> getters, int fetchRows,
                                         int queryTimeout, boolean isPreview) throws SQLException
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

    @Override
    protected void setSearchPath(String schema) throws SQLException
    {
        // NOP
    }

    @Override
    protected String buildTableName(String tableName)
    {
        StringBuilder sb = new StringBuilder();
        if (schemaName != null) {
            sb.append(quoteIdentifierString(schemaName)).append(".");
        }
        sb.append(quoteIdentifierString(tableName));
        if (transactionIsolationLevel != null) {
            sb.append(" with (" + transactionIsolationLevel + ")");
        }
        return sb.toString();
    }
}
