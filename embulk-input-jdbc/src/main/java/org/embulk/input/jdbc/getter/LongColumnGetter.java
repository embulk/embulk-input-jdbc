package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class LongColumnGetter
        extends AbstractColumnGetter
{
    protected long value;

    public LongColumnGetter(PageBuilder to, Type toType)
    {
        super(to, toType);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        value = from.getLong(fromIndex);
    }

    @Override
    protected Type getDefaultToType()
    {
        return Types.LONG;
    }

    @Override
    public void booleanColumn(Column column)
    {
        to.setBoolean(column, value > 0L);
    }

    @Override
    public void longColumn(Column column)
    {
        to.setLong(column, value);
    }

    @Override
    public void doubleColumn(Column column)
    {
        to.setDouble(column, value);
    }

    @Override
    public void stringColumn(Column column)
    {
        to.setString(column, Long.toString(value));
    }

    @Override
    public JsonNode encodeToJson()
    {
        return jsonNodeFactory.numberNode(value);
    }

    @Override
    public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
        throws SQLException
    {
        toStatement.setLong(toIndex, fromValue.asLong());
    }
}
