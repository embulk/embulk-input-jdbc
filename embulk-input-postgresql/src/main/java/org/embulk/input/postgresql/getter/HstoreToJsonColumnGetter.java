package org.embulk.input.postgresql.getter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.embulk.input.jdbc.getter.JsonColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.type.Type;
import org.msgpack.value.Value;
import org.postgresql.util.HStoreConverter;

import java.util.Map;

public class HstoreToJsonColumnGetter
        extends JsonColumnGetter
{
    private final ObjectMapper mapper = new ObjectMapper();

    public HstoreToJsonColumnGetter(PageBuilder to, Type toType)
    {
        super(to, toType);
    }

    @Override
    public void jsonColumn(Column column)
    {
        Value v;
        try {
            Map map = HStoreConverter.fromString(value);
            v = jsonParser.parse(mapper.writeValueAsString(map));
        } catch (JsonProcessingException | JsonParseException e) {
            super.jsonColumn(column);
            return;
        }
        to.setJson(column, v);
    }
}
