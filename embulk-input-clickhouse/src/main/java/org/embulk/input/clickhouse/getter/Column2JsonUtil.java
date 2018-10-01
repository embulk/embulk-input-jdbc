package org.embulk.input.clickhouse.getter;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParser;
import org.msgpack.value.Value;

import java.sql.SQLException;

public class Column2JsonUtil {
    private final Gson gson = new Gson();
    private final JsonParser jsonParser = new JsonParser();

    public void jsonColumn(Column column, PageBuilder to, Object value) throws JsonParseException, SQLException, ClassCastException {
        String jsonString = gson.toJson(value);
        Value v = jsonParser.parse(jsonString);
        to.setJson(column, v);
    }

    public void stringColumn(Column column, PageBuilder to, Object value) {
        String jsonString = gson.toJson(value);
        to.setString(column, jsonString);
    }
}
