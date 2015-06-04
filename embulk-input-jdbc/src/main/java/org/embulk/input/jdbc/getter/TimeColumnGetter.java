package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class TimeColumnGetter
        extends AbstractColumnGetter
{
    private Time value;

    public TimeColumnGetter(PageBuilder to)
    {
        super(to);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        value = from.getTime(fromIndex);
    }

    @Override
    public Type getToType()
    {
        return Types.TIMESTAMP.withFormat("%H:%M:%S");
    }

    @Override
    public void stringColumn(Column column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void timestampColumn(Column column)
    {
        Timestamp t = Timestamp.ofEpochMilli(value.getTime());
        to.setTimestamp(column, t);
    }
}
