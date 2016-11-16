package org.embulk.input.mysql.getter;

import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.TimestampWithTimeZoneIncrementalHandler;
import org.embulk.spi.Column;
import org.joda.time.DateTimeZone;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class MySQLTimestampIncrementalHandler
        extends TimestampWithTimeZoneIncrementalHandler
{
    private DateTimeZone sessionTimeZone;

    public MySQLTimestampIncrementalHandler(ColumnGetter next)
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
        epochSecond = sessionTimeZone.convertLocalToUTC(timestamp.getTime(), false) / 1000;
        nano = timestamp.getNanos();
    }
}
