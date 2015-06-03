package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class StringColumnGetter
        extends AbstractColumnGetter
{
    private String value;

    public StringColumnGetter(PageBuilder to)
    {
        super(to);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        value = from.getString(fromIndex);
    }

    @Override
    public Type getToType()
    {
        return Types.STRING;
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
        to.setString(column, value);
    }

    @Override
    public void timestampColumn(Column column)
    {
        throw new UnsupportedOperationException();
    }
}
