package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.embulk.input.jdbc.getter.AbstractColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;

public class JsonColumnGetter
        extends AbstractColumnGetter
{
    protected final JsonParser jsonParser = new JsonParser();

    protected String value;

    public JsonColumnGetter(PageBuilder to, Type toType)
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
        return Types.JSON;
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
