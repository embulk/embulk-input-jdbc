package org.embulk.input.mysql.getter;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.TimestampWithTimeZoneIncrementalHandler;
import org.embulk.spi.Column;
import org.joda.time.DateTimeZone;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class MySQLDateTimeIncrementalHandler
        extends TimestampWithTimeZoneIncrementalHandler
{
    private DateTimeZone sessionTimeZone;

    public MySQLDateTimeIncrementalHandler(ColumnGetter next)
    {
        super(next);
    }

    @Override
    public void getAndSet(ResultSet from, int fromIndex, Column toColumn)
            throws SQLException
    {
        if (sessionTimeZone == null) {
            sessionTimeZone = MySQLColumnGetterFactory.getSessionTimeZone(from);
        }
        super.getAndSet(from, fromIndex, toColumn);
    }

    @Override
    public void getAndSet(Timestamp timestamp)
    {
        // this Timestamp value is already converted by session time_zone.
        epochSecond = sessionTimeZone.convertUTCToLocal(timestamp.getTime()) / 1000; // reconvert from session time_zone to UTC
        nano = timestamp.getNanos();
    }

    @Override
    public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
            throws SQLException
    {
        if (sessionTimeZone == null) {
            sessionTimeZone = MySQLColumnGetterFactory.getSessionTimeZone(toStatement);
        }
        super.decodeFromJsonTo(toStatement, toIndex, fromValue);
    }

    @Override
    public Timestamp toSqlTimestamp(org.embulk.spi.time.Timestamp from)
    {
        // reconvert from UTC to session time_zone
        Timestamp sqlTimestamp = new Timestamp(sessionTimeZone.convertLocalToUTC(from.getEpochSecond() * 1000, false));
        sqlTimestamp.setNanos(from.getNano());
        return sqlTimestamp;
    }
}
