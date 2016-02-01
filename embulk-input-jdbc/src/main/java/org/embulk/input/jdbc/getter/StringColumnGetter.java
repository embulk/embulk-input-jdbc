package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;

public class StringColumnGetter
        extends AbstractColumnGetter
{
    private static final JsonParser jsonParser = new JsonParser();

    private String value;

    public StringColumnGetter(PageBuilder to, Type toType)
    {
        super(to, toType);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        value = from.getString(fromIndex);
    }

    @Override
    protected Type getDefaultToType()
    {
        return Types.STRING;
    }

    @Override
    public void longColumn(Column column)
    {
        long l;
        try {
            l = Long.parseLong(value);
        } catch (NumberFormatException e) {
            super.longColumn(column);
            return;
        }
        to.setLong(column, l);
    }

    @Override
    public void doubleColumn(Column column)
    {
        double d;
        try {
            d = Double.parseDouble(value);
        } catch (NumberFormatException e) {
            super.doubleColumn(column);
            return;
        }
        to.setDouble(column, d);
    }

    @Override
    public void jsonColumn(Column column)
    {
        Value v;
        try {
            v = jsonParser.parse(value);
        } catch (JsonParseException e) {
            super.jsonColumn(column);
            return;
        }
        to.setJson(column, v);
    }

    @Override
    public void stringColumn(Column column)
    {
        to.setString(column, value);
    }

}
