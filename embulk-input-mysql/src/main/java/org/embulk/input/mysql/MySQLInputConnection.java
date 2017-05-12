package org.embulk.input.mysql;

import java.util.List;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.TimeZone;

import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.ConnectionProperties;
import org.embulk.input.MySQLTimeZoneComparison;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.JdbcLiteral;
import org.embulk.input.jdbc.getter.ColumnGetter;

public class MySQLInputConnection
        extends JdbcInputConnection
{
    public MySQLInputConnection(Connection connection)
            throws SQLException
    {
        super(connection, null);
    }

    @Override
    protected BatchSelect newBatchSelect(PreparedQuery preparedQuery,
            List<ColumnGetter> getters,
            int fetchRows, int queryTimeout) throws SQLException
    {
        String query = preparedQuery.getQuery();
        List<JdbcLiteral> params = preparedQuery.getParameters();

        logger.info("SQL: " + query);
        PreparedStatement stmt = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);  // TYPE_FORWARD_ONLY and CONCUR_READ_ONLY are default
        if (!params.isEmpty()) {
            logger.info("Parameters: {}", params);
            prepareParameters(stmt, getters, params);
        }
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
        // Because socketTimeout is set in Connection, don't need to set quertyTimeout.
        return new SingleSelect(stmt);
    }

    public boolean getUseLegacyDatetimeCode()
    {
        return ((ConnectionProperties) connection).getUseLegacyDatetimeCode();
    }

    public TimeZone getServerTimezoneTZ()
    {
        return ((ConnectionImpl) connection).getServerTimezoneTZ();
    }

    public void compareTimeZone() throws SQLException
    {
        MySQLTimeZoneComparison timeZoneComparison = new MySQLTimeZoneComparison(connection);
        timeZoneComparison.compareTimeZone();
    }

}
