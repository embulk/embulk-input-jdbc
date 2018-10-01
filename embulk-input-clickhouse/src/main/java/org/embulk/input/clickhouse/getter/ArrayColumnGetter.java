package org.embulk.input.clickhouse.getter;

import org.embulk.input.jdbc.getter.AbstractColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.type.Type;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Supported JSON or String
 */
public class ArrayColumnGetter extends AbstractColumnGetter
{
    protected Array value;
    Column2JsonUtil c2j = new Column2JsonUtil();

    public ArrayColumnGetter(PageBuilder to, Type toType)
    {
        super(to, toType);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        value = from.getArray(fromIndex);
    }

    @Override
    protected Type getDefaultToType()
    {
        return org.embulk.spi.type.Types.JSON;
    }

    @Override
    public void jsonColumn(Column column)
    {
        try {
            c2j.jsonColumn(column, to, value.getArray());
        }
        catch (JsonParseException | SQLException | ClassCastException e) {
            super.jsonColumn(column);
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        try {
            c2j.stringColumn(column, to, value.getArray());
        }
        catch (SQLException e) {
            to.setString(column, value.toString());
        }
    }
}
