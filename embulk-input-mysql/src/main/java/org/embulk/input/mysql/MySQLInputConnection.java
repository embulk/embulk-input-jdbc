package org.embulk.input.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import org.embulk.input.jdbc.JdbcInputConnection;

public class MySQLInputConnection
        extends JdbcInputConnection
{
    public MySQLInputConnection(Connection connection)
            throws SQLException
    {
        super(connection, null);
    }

    @Override
    protected BatchSelect newBatchSelect(String select, int fetchRows) throws SQLException
    {
        logger.info("SQL: " + select);
        PreparedStatement stmt = connection.prepareStatement(select, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);  // TYPE_FORWARD_ONLY and CONCUR_READ_ONLY are default
        if (fetchRows == 1) {
            // See MySQLInputPlugin.newConnection doesn't set useCursorFetch=true when fetchRows=1
            // MySQL Connector/J keeps the connection opened and process rows one by one with Integer.MIN_VALUE.
            stmt.setFetchSize(Integer.MIN_VALUE);
        } else if (fetchRows <= 0) {
            // uses the default behavior. MySQL Connector/J fetches the all rows in memory.
        } else {
            // useCursorFetch=true is enabled. MySQL creates temporary table and uses multiple select statements to fetch rows.
            stmt.setFetchSize(fetchRows);
        }
        return new SingleSelect(stmt);
    }
}
