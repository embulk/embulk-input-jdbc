package org.embulk.input.mysql.getter;

import org.embulk.input.jdbc.getter.ColumnGetter;

import java.sql.Timestamp;
import java.time.ZoneId;

public class MySQLTimestampTimestampIncrementalHandler
        extends AbstractMySQLTimestampIncrementalHandler
{
    public MySQLTimestampTimestampIncrementalHandler(final ZoneId sessionTimeZone, final ColumnGetter next)
    {
        super(sessionTimeZone, next);
    }

    @Override
    public String getTimestampFormat()
    {
        return "%Y-%m-%dT%H:%M:%S.%6NZ";
    }

    @Override
    public org.embulk.spi.time.Timestamp utcTimestampFromSessionTime(long epochSecond, int nano)
    {
        return org.embulk.spi.time.Timestamp.ofEpochSecond(epochSecond, nano);
    }

    @Override
    public String getTimestampPattern()
    {
        return "%Y-%m-%dT%H:%M:%S.%N%z";
    }

    @Override
    public Timestamp utcTimestampToSessionTime(org.embulk.spi.time.Timestamp ts)
    {
        Timestamp sqlTimestamp = new Timestamp(ts.getEpochSecond() * 1000);
        sqlTimestamp.setNanos(ts.getNano());
        return sqlTimestamp;
    }
}
