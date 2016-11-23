package org.embulk.input.mysql.getter;

import org.embulk.input.jdbc.getter.ColumnGetter;
import org.joda.time.DateTimeZone;

import java.sql.Timestamp;

public class MySQLDateTimeIncrementalHandler
        extends AbstractMySQLIncrementalHandler
{
    public MySQLDateTimeIncrementalHandler(DateTimeZone sessionTimeZone, ColumnGetter next)
    {
        super(sessionTimeZone, next);
    }

    @Override
    public String getTimestampFormat()
    {
        return "%Y-%m-%dT%H:%M:%S.%6N";
    }

    @Override
    public org.embulk.spi.time.Timestamp convertTimestamp(long epochSecond, int nano)
    {
        // this Timestamp value is already converted by session time_zone.
        long reconverted = sessionTimeZone.convertUTCToLocal(epochSecond * 1000) / 1000; // reconvert from session time_zone to UTC
        return org.embulk.spi.time.Timestamp.ofEpochSecond(reconverted, nano);
    }

    @Override
    public String getTimestampPattern()
    {
        return "%Y-%m-%dT%H:%M:%S.%N";
    }

    @Override
    public Timestamp convertTimestamp(org.embulk.spi.time.Timestamp from)
    {
        // reconvert from UTC to session time_zone
        long reconverted = sessionTimeZone.convertLocalToUTC(from.getEpochSecond() * 1000, false);
        Timestamp sqlTimestamp = new Timestamp(reconverted);
        sqlTimestamp.setNanos(from.getNano());
        return sqlTimestamp;
    }
}
