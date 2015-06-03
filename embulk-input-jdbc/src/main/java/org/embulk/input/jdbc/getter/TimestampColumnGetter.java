package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class TimestampColumnGetter
        extends AbstractColumnGetter
{
    private java.sql.Timestamp value;

    public TimestampColumnGetter(PageBuilder to)
    {
        super(to);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        value = from.getTimestamp(fromIndex);
    }

    @Override
    public Type getToType()
    {
        return Types.TIMESTAMP.withFormat("%Y-%m-%d %H:%M:%S");
    }

    @Override
    public void booleanColumn(Column column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void longColumn(Column column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void doubleColumn(Column column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stringColumn(Column column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void timestampColumn(Column column)
    {
        Timestamp t = Timestamp.ofEpochSecond(value.getTime() / 1000, value.getNanos());
        to.setTimestamp(column, t);
    }
}
