package org.embulk.input.mysql.getter;

import org.embulk.input.jdbc.getter.ColumnGetter;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class MySQLDateTimeTimestampIncrementalHandler
        extends AbstractMySQLTimestampIncrementalHandler
{
    public MySQLDateTimeTimestampIncrementalHandler(final ZoneId sessionTimeZone, final ColumnGetter next)
    {
        super(sessionTimeZone, next);
    }

    @Override
    public String getTimestampFormat()
    {
        return "%Y-%m-%dT%H:%M:%S.%6N";
    }

    @Override
    public org.embulk.spi.time.Timestamp utcTimestampFromSessionTime(long epochSecond, int nano)
    {
        // this Timestamp value is already converted by session time_zone.
        final long reconverted = ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), this.sessionTimeZone).
                withZoneSameLocal(ZoneOffset.UTC).toEpochSecond();  // reconvert from session time_zone to UTC
        return org.embulk.spi.time.Timestamp.ofEpochSecond(reconverted, nano);
    }

    @Override
    public String getTimestampPattern()
    {
        return "%Y-%m-%dT%H:%M:%S.%N";
    }

    @Override
    public Timestamp utcTimestampToSessionTime(org.embulk.spi.time.Timestamp from)
    {
        // reconvert from UTC to session time_zone
        final long reconverted = ZonedDateTime.ofInstant(Instant.ofEpochSecond(from.getEpochSecond()), ZoneOffset.UTC)
                .withZoneSameLocal(this.sessionTimeZone).toEpochSecond() * 1000;
        Timestamp sqlTimestamp = new Timestamp(reconverted);
        sqlTimestamp.setNanos(from.getNano());
        return sqlTimestamp;
    }
}
