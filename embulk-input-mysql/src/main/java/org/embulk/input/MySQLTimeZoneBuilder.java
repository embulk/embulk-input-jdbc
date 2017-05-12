package org.embulk.input;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.TimeZone;

public class MySQLTimeZoneBuilder
{
    private static final int ONE_HOUR_SEC = 3600;
    private static final int ONE_MIN_SEC = 60;

    public static TimeZone fromSystemTimeZone(Connection connection) throws SQLException
    {
        //
        // First, I used `@@system_time_zone`. but It return non Time Zone Abbreviations name on a specific platform.
        // So, This method calculate GMT offset with query.
        //
        String query = "select TIME_TO_SEC(timediff(now(),utc_timestamp()));";
        Statement stmt = connection.createStatement();

        try {
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
                int offsetSeconds = rs.getInt(1);
                return fromGMTOffsetSeconds(offsetSeconds);
            }
            else {
                // TODO Error check.
                return null;
            }

        } finally {
            stmt.close();
        }
    }

    private static TimeZone fromGMTOffsetSeconds(int offsetSeconds)
    {
        if( offsetSeconds == 0 ) {
            return TimeZone.getTimeZone("UTC");
        }

        String sign = offsetSeconds > 0 ? "+" : "-";
        int absOffsetSec = Math.abs(offsetSeconds);
        int tzHour = absOffsetSec / ONE_HOUR_SEC;
        int tzMin = absOffsetSec % ONE_HOUR_SEC / ONE_MIN_SEC;
        String tzName = String.format(Locale.ENGLISH, "GMT%s%02d:%02d", sign, tzHour, tzMin);
        return TimeZone.getTimeZone(tzName);
    }
}
