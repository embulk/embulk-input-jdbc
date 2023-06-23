package org.embulk.input.mysql;

import java.util.List;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.TimeZone;

import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.JdbcLiteral;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLInputConnection
        extends JdbcInputConnection
{
    private static final Logger logger = LoggerFactory.getLogger(MySQLInputConnection.class);

    public MySQLInputConnection(Connection connection)
            throws SQLException
    {
        super(connection, null);
    }

    @Override
    protected BatchSelect newBatchSelect(PreparedQuery preparedQuery, List<ColumnGetter> getters, int fetchRows,
                                         int queryTimeout, boolean isPreview) throws SQLException
    {
        String query = preparedQuery.getQuery();

        List<JdbcLiteral> params = preparedQuery.getParameters();

        logger.info("SQL: " + query);
        PreparedStatement stmt = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);  // TYPE_FORWARD_ONLY and CONCUR_READ_ONLY are default

        if (!params.isEmpty()) {
            logger.info("Parameters: {}", params);
            prepareParameters(stmt, getters, params);
        }

        if (isPreview) {
            stmt.setMaxRows(MAX_PREVIEW_RECORDS);
            stmt.setFetchSize(MAX_PREVIEW_RECORDS);
        }
        else {
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
        }
        // Because socketTimeout is set in Connection, don't need to set quertyTimeout.
        return new SingleSelect(stmt);
    }

    public boolean getUseLegacyDatetimeCode()
    {
        try {
            Class<?> connectionPropertiesClass = Class.forName("com.mysql.jdbc.ConnectionProperties");
            Method getUseLegacyDatetimeCodeMethod = connectionPropertiesClass.getMethod("getUseLegacyDatetimeCode");
            return (Boolean)getUseLegacyDatetimeCodeMethod.invoke(connection);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public TimeZone getServerTimezoneTZ()
    {
        try {
            Class<?> connectionImplClass = Class.forName("com.mysql.jdbc.ConnectionImpl");
            Method getServerTimezoneTZMethod = connectionImplClass.getMethod("getServerTimezoneTZ");
            return (TimeZone)getServerTimezoneTZMethod.invoke(connection);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void showDriverVersion() throws SQLException {
        super.showDriverVersion();
        logger.warn("embulk-input-mysql 0.9.0 upgraded the bundled MySQL Connector/J version from 5.1.34 to 5.1.44 .");
        logger.warn("And set useLegacyDatetimeCode=false by default in order to get correct datetime value when the server timezone and the client timezone are different.");
        logger.warn("Set useLegacyDatetimeCode=true if you need to get datetime value same as older embulk-input-mysql.");
    }
}
