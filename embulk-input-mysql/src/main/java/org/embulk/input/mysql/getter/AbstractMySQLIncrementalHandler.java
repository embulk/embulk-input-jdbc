package org.embulk.input.mysql.getter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.ResultSetImpl;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.TimestampWithTimeZoneIncrementalHandler;
import org.embulk.spi.Column;
import org.joda.time.DateTimeZone;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

public class AbstractMySQLIncrementalHandler
        extends TimestampWithTimeZoneIncrementalHandler
{
    protected DateTimeZone sessionTimeZone;

    public AbstractMySQLIncrementalHandler(ColumnGetter next)
    {
        super(next);
    }

    @Override
    public void getAndSet(ResultSet from, int fromIndex,
            Column toColumn) throws SQLException
    {
        if (sessionTimeZone == null) {
            sessionTimeZone = getSessionTimeZone(from);
        }
        super.getAndSet(from, fromIndex, toColumn); // sniff the value
    }

    @Override
    public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
            throws SQLException
    {
        if (sessionTimeZone == null) {
            sessionTimeZone = getSessionTimeZone(toStatement);
        }
        super.decodeFromJsonTo(toStatement, toIndex, fromValue);
    }

    private DateTimeZone getSessionTimeZone(ResultSet from)
    {
        Field f = null;
        try {
            // Need to check if the processing works fine or not to upgrade mysql-connector-java version.
            f = ResultSetImpl.class.getDeclaredField("serverTimeZoneTz");
            f.setAccessible(true);
            TimeZone timeZone = (TimeZone) f.get(from);

            // Joda-Time's timezone mapping is probably not compatible with java.util.TimeZone if null is returned.
            return Preconditions.checkNotNull(DateTimeZone.forTimeZone(timeZone));
        }
        catch (IllegalAccessException | NoSuchFieldException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if (f != null) {
                f.setAccessible(false);
            }
        }
    }

    private DateTimeZone getSessionTimeZone(java.sql.PreparedStatement from)
            throws SQLException
    {
        TimeZone timeZone = ((ConnectionImpl) from.getConnection()).getServerTimezoneTZ();
        // Joda-Time's timezone mapping is probably not compatible with java.util.TimeZone if null is returned.
        return Preconditions.checkNotNull(DateTimeZone.forTimeZone(timeZone));
    }
}
