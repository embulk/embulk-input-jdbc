package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class LongColumnGetter
        extends AbstractColumnGetter
{
    private long value;

    public LongColumnGetter(PageBuilder to)
    {
        super(to);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        value = from.getLong(fromIndex);
    }

    @Override
    public Type getToType()
    {
        return Types.LONG;
    }

    @Override
    public void booleanColumn(Column column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void longColumn(Column column)
    {
        to.setLong(column, value);
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
        throw new UnsupportedOperationException();
    }
}
