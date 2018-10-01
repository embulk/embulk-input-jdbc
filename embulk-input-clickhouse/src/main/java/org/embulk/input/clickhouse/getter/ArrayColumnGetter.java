package org.embulk.input.clickhouse.getter;

import com.google.gson.Gson;
import org.embulk.input.jdbc.getter.AbstractColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.type.Type;
import org.msgpack.value.Value;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Supported JSON or String
 */
public class ArrayColumnGetter extends AbstractColumnGetter
{
    protected Array value;

    private final Gson gson = new Gson();
    private final JsonParser jsonParser = new JsonParser();

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
            String jsonString = gson.toJson(value.getArray());
            Value v = jsonParser.parse(jsonString);
            to.setJson(column, v);
        }
        catch (JsonParseException | SQLException | ClassCastException e) {
            super.jsonColumn(column);
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        try {
            String jsonString = gson.toJson(value.getArray());
            to.setString(column, jsonString);
        }
        catch (SQLException e) {
            to.setString(column, value.toString());
        }
    }
}
