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
    public Instant utcTimestampFromSessionTime(final long epochSecond, final int nano)
    {
        // this Timestamp value is already converted by session time_zone.
        final long reconverted = ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), this.sessionTimeZone).
                withZoneSameLocal(ZoneOffset.UTC).toEpochSecond();  // reconvert from session time_zone to UTC
        return Instant.ofEpochSecond(reconverted, nano);
    }

    @Override
    public String getTimestampPattern()
    {
        return "%Y-%m-%dT%H:%M:%S.%N";
    }

    @Override
    public Timestamp utcTimestampToSessionTime(final Instant from)
    {
        // reconvert from UTC to session time_zone
        final long reconverted = ZonedDateTime.ofInstant(from, ZoneOffset.UTC)
                .withZoneSameLocal(this.sessionTimeZone).toEpochSecond() * 1000;
        Timestamp sqlTimestamp = new Timestamp(reconverted);
        sqlTimestamp.setNanos(from.getNano());
        return sqlTimestamp;
    }
}
