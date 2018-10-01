package org.embulk.input.clickhouse.getter;

import org.embulk.input.jdbc.getter.AbstractColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Tuple is provided as String value.
 */
public class TupleColumnGetter extends AbstractColumnGetter
{
    protected String value;
    Column2JsonUtil c2j = new Column2JsonUtil();

    public TupleColumnGetter(PageBuilder to, Type toType)
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
    public void jsonColumn(Column column)
    {
        //FIXME: convert to flat array as JSON
        try {
            c2j.jsonColumn(column, to, value);
        }
        catch (JsonParseException | SQLException | ClassCastException e) {
            super.jsonColumn(column);
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        to.setString(column, value);
    }
}
