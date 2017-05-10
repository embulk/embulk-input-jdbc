package org.embulk.input;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.TimeZone;

public class MySQLTimeZoneBuilder
{
    public static final int ONE_HOUR_SEC = 3600;
    public static final int ONE_MIN_SEC = 60;

    public static TimeZone fromSystemTimeZone(Connection connection)
        throws SQLException
    {
        //
        // First, I used `@@system_time_zone`. but It return non Time Zone Abbreviations name on a specific platform.
        // So, This method calculate GMT offset with query.
        //
        String query = "select TIME_TO_SEC(timediff(now(),utc_timestamp()));";
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);

        if (rs.next()) {
            int offset_seconds = rs.getInt(1);
            return fromGMTOffsetSeconds(offset_seconds);
        }
        else {
            // TODO Error check.
            return null;
        }
    }

    public static TimeZone fromGMTOffsetSeconds(int offset_seconds)
    {
        String sign = offset_seconds > 0 ? "+" : "-";
        int abs_offset_sec =  Math.abs(offset_seconds);
        int tz_hour =  abs_offset_sec / ONE_HOUR_SEC;
        int tz_min =  abs_offset_sec % ONE_HOUR_SEC / ONE_MIN_SEC;
        String tz_name = String.format(Locale.ENGLISH,"GMT%s%02d:%02d",sign,tz_hour,tz_min);

        return TimeZone.getTimeZone(tz_name);
    }
}
