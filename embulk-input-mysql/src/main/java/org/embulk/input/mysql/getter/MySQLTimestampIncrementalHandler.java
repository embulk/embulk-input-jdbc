package org.embulk.input.mysql.getter;

import org.embulk.input.jdbc.getter.ColumnGetter;

import static com.google.common.base.Preconditions.checkNotNull;

public class MySQLTimestampIncrementalHandler
        extends AbstractMySQLIncrementalHandler
{
    public MySQLTimestampIncrementalHandler(ColumnGetter next)
    {
        super(next);
    }

    @Override
    public org.embulk.spi.time.Timestamp toEmbulkTimestamp(long epochSecond, int nano)
    {
        checkNotNull(sessionTimeZone);
        long sec = sessionTimeZone.convertLocalToUTC(epochSecond * 1000, false) / 1000;
        return org.embulk.spi.time.Timestamp.ofEpochSecond(sec, nano);
    }
}
