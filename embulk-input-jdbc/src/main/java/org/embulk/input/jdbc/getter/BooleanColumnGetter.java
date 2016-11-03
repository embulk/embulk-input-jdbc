package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class BooleanColumnGetter
        extends AbstractColumnGetter
{
    protected boolean value;

    public BooleanColumnGetter(PageBuilder to, Type toType)
    {
        super(to, toType);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        value = from.getBoolean(fromIndex);
    }

    @Override
    protected Type getDefaultToType()
    {
        return Types.BOOLEAN;
    }

    @Override
    public void booleanColumn(Column column)
    {
        to.setBoolean(column, value);
    }

    @Override
    public void longColumn(Column column)
    {
        to.setLong(column, value ? 1L : 0L);
    }

    @Override
    public void doubleColumn(Column column)
    {
        to.setDouble(column, value ? 1.0 : 0.0);
    }

    @Override
    public void stringColumn(Column column)
    {
        to.setString(column, Boolean.toString(value));
    }

}
