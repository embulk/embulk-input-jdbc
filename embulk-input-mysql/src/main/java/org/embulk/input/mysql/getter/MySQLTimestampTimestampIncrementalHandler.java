package org.embulk.input.mysql.getter;

import org.embulk.input.jdbc.getter.ColumnGetter;

import java.sql.Timestamp;
import java.time.Instant;
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
    public Instant utcTimestampFromSessionTime(final long epochSecond, final int nano)
    {
        return Instant.ofEpochSecond(epochSecond, nano);
    }

    @Override
    public String getTimestampPattern()
    {
        return "%Y-%m-%dT%H:%M:%S.%N%z";
    }

    @Override
    public Timestamp utcTimestampToSessionTime(final Instant ts)
    {
        Timestamp sqlTimestamp = new Timestamp(ts.getEpochSecond() * 1000);
        sqlTimestamp.setNanos(ts.getNano());
        return sqlTimestamp;
    }
}
