package org.embulk.input.postgresql;

import java.sql.Statement;
import java.util.List;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.JdbcLiteral;
import org.embulk.input.jdbc.getter.ColumnGetter;

public class PostgreSQLInputConnection
        extends JdbcInputConnection
{
    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLInputConnection.class);

    public PostgreSQLInputConnection(Connection connection, String schemaName, Optional<Integer> statementTimeoutMillis)
            throws SQLException
    {
        super(connection, schemaName);
        setStatementTimeoutIfSpecified(statementTimeoutMillis);
    }

    @Override
    protected BatchSelect newBatchSelect(PreparedQuery preparedQuery, List<ColumnGetter> getters, int fetchRows,
                                         int queryTimeout, boolean isPreview) throws SQLException
    {
        String query = preparedQuery.getQuery();

        query = "DECLARE cur NO SCROLL CURSOR FOR " + query;

        List<JdbcLiteral> params = preparedQuery.getParameters();

        logger.info("SQL: " + query);
        PreparedStatement stmt = connection.prepareStatement(query);

        String fetchSql = "FETCH FORWARD "+fetchRows+" FROM cur";

        if (isPreview) {
            stmt.setMaxRows(MAX_PREVIEW_RECORDS);
            fetchSql = "FETCH FORWARD "+MAX_PREVIEW_RECORDS+" FROM cur";
        }

        try {
            if (!params.isEmpty()) {
                logger.info("Parameters: {}", params);
                prepareParameters(stmt, getters, params);
            }
            stmt.executeUpdate();
        } finally {
            stmt.close();
        }

        // Because socketTimeout is set in Connection, don't need to set quertyTimeout.
        return new CursorSelect(fetchSql, connection.prepareStatement(fetchSql));
    }

    public class CursorSelect
            implements BatchSelect
    {
        private final String fetchSql;
        private final PreparedStatement fetchStatement;

        public CursorSelect(String fetchSql, PreparedStatement fetchStatement) throws SQLException
        {
            this.fetchSql = fetchSql;
            this.fetchStatement = fetchStatement;
        }

        public ResultSet fetch() throws SQLException
        {
            logger.info("SQL: " + fetchSql);
            long startTime = System.currentTimeMillis();

            ResultSet rs = fetchStatement.executeQuery();

            double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
            logger.info(String.format("> %.2f seconds", seconds));
            return rs;
        }

        public void close() throws SQLException
        {
            // TODO close?
        }
    }

    private void setStatementTimeoutIfSpecified(Optional<Integer> statementTimeoutMillis)
        throws SQLException
    {
        if (statementTimeoutMillis.isPresent() && statementTimeoutMillis.get() > 0) {
            Statement stmt = connection.createStatement();
            try {
                String sql = "SET statement_timeout TO " + quoteIdentifierString(String.valueOf(statementTimeoutMillis.get()));
                executeUpdate(sql);
            }
            finally {
                stmt.close();
            }
        }
    }
}
