package org.embulk.input.mysql.getter;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.TimestampWithTimeZoneIncrementalHandler;
import org.embulk.spi.Column;
import org.joda.time.DateTimeZone;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
            sessionTimeZone = MySQLColumnGetterFactory.getSessionTimeZone(from);
        }
        super.getAndSet(from, fromIndex, toColumn); // sniff the value
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
}
