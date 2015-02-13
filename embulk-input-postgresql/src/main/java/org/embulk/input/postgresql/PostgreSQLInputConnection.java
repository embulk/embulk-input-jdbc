package org.embulk.input.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.embulk.spi.Exec;
import org.embulk.input.jdbc.JdbcInputConnection;

public class PostgreSQLInputConnection
        extends JdbcInputConnection
{
    private final Logger logger = Exec.getLogger(PostgreSQLInputConnection.class);

    public PostgreSQLInputConnection(Connection connection, String schemaName)
            throws SQLException
    {
        super(connection, schemaName);
    }

    @Override
    protected CursorSelect newBatchSelect(String select, int fetchRows) throws SQLException
    {
        executeUpdate("DECLARE cur NO SCROLL CURSOR FOR "+select);

        String fetchSql = "FETCH FORWARD "+fetchRows+" FROM cur";
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
}
