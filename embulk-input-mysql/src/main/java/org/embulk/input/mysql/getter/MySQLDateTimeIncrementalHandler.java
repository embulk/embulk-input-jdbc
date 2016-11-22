package org.embulk.input.mysql.getter;

import org.embulk.input.jdbc.getter.ColumnGetter;

import java.sql.Timestamp;

import static com.google.common.base.Preconditions.checkNotNull;

public class MySQLDateTimeIncrementalHandler
        extends AbstractMySQLIncrementalHandler
{
    public MySQLDateTimeIncrementalHandler(ColumnGetter next)
    {
        super(next);
    }

    @Override
    public String getUsecFormat()
    {
        return "%Y-%m-%d %H:%M:%S.%6N";
    }

    @Override
    public org.embulk.spi.time.Timestamp toEmbulkTimestamp(long epochSecond, int nano)
    {
        checkNotNull(sessionTimeZone);
        // this Timestamp value is already converted by session time_zone.
        long reconverted = sessionTimeZone.convertUTCToLocal(epochSecond * 1000) / 1000; // reconvert from session time_zone to UTC
        return super.toEmbulkTimestamp(reconverted, nano);
    }

    @Override
    public String getUsecPattern()
    {
        return "%Y-%m-%d %H:%M:%S.%N";
    }

    @Override
    public Timestamp toSqlTimestamp(org.embulk.spi.time.Timestamp from)
    {
        checkNotNull(sessionTimeZone);
        // reconvert from UTC to session time_zone
        long reconverted = sessionTimeZone.convertLocalToUTC(from.getEpochSecond() * 1000, false);
        Timestamp sqlTimestamp = new Timestamp(reconverted);
        sqlTimestamp.setNanos(from.getNano());
        return sqlTimestamp;
    }
}
