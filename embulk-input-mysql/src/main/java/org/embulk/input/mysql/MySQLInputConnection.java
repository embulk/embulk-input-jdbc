package org.embulk.input.mysql;

import java.sql.Statement;
import java.util.List;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.Locale;
import java.util.TimeZone;

import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.ConnectionProperties;
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

    @Override
    public void before_load()
        throws SQLException
    {
        compareTimeZoneOffset();
    }

    private int getServerTimeZoneOffsetInSeconds(){

        //
        // First, I used `@@system_time_zone`. but It return non Time Zone Abbreviations name on a specific platform.
        // So, This method get GMT offset with query.
        //
        String query = "select TIME_TO_SEC(timediff(now(),utc_timestamp()));";

        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            if (rs.next()) {
                int offset = rs.getInt(1);
                logger.info(String.format(Locale.ENGLISH,"Server timezone offset in seconds: %d",offset));
                return offset;
            }
            else {
                // TODO Error Test.
                return -1;
            }
        }
        catch (SQLException ex) {
            // TODO
            return -1;
        }
    }

    private void compareTimeZoneOffset(){

        int tz_offset_sec = getServerTimeZoneOffsetInSeconds();
        String svr_tz_name = makeTimeZoneOffsetFromSeconds(tz_offset_sec);
        TimeZone svr_tz = TimeZone.getTimeZone(svr_tz_name);

        String usr_tz_name = System.getProperty("user.timezone");
        TimeZone usr_tz = TimeZone.getTimeZone(usr_tz_name);

        if( svr_tz.getRawOffset() != usr_tz.getRawOffset() ) {
            logger.warn(String.format(Locale.ENGLISH,
                    "The server timezone offset(%s) and client timezone(%s) has different timezone offset. The plugin will fetch wrong datetime values.",svr_tz_name,usr_tz_name));
            logger.warn(String.format(Locale.ENGLISH,
                    "Use `options: { useLegacyDatetimeCode: false }`"));
        }
        logger.warn(String.format(Locale.ENGLISH,"The plugin will set `useLegacyDatetimeCode=false` by default in future."));

    }

    private String makeTimeZoneOffsetFromSeconds(int tz_offset_sec)
    {
        if( tz_offset_sec == 0 ){
            return "UTC";
        }
        else {
            int hour_sec = 3600;
            int min_sec = 60;

            String sign = tz_offset_sec > 0 ? "+" : "-";
            int abs_offset_sec =  Math.abs(tz_offset_sec);
            int tz_hour =  abs_offset_sec / hour_sec;
            int tz_min =  abs_offset_sec % hour_sec / min_sec;
            return String.format(Locale.ENGLISH,"GMT%s%02d:%02d",sign,tz_hour,tz_min);
        }
    }
}
