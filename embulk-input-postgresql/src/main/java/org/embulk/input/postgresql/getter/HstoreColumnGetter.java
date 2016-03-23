package org.embulk.input.postgresql.getter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.embulk.input.jdbc.getter.AbstractColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.postgresql.util.HStoreConverter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class HstoreColumnGetter
        extends AbstractColumnGetter
{
    final JsonParser parser = new JsonParser();
    final ObjectMapper mapper = new ObjectMapper();

    private String value;

    public HstoreColumnGetter(PageBuilder to, Type toType)
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
        Value v;
        try {
            Map map = HStoreConverter.fromString(value);
            v = parser.parse(mapper.writeValueAsString(map));
        } catch (JsonProcessingException | JsonParseException e) {
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
