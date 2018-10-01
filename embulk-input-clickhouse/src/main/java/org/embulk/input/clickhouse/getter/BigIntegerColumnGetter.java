package org.embulk.input.clickhouse.getter;

import com.google.gson.Gson;
import org.embulk.input.jdbc.getter.AbstractColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import ru.yandex.clickhouse.response.ClickHouseResultSet;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BigIntegerColumnGetter extends AbstractColumnGetter {

    protected BigInteger value;

    private final Gson gson = new Gson();
    private final JsonParser jsonParser = new JsonParser();

    private String originalColumnTypeName;

    public BigIntegerColumnGetter(PageBuilder to, Type toType, String originalColumnTypeName) {
        super(to, toType);
        this.originalColumnTypeName = originalColumnTypeName;
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException {
        ClickHouseResultSet chFrom = (ClickHouseResultSet)from;
        value = (BigInteger)chFrom.getObject(fromIndex);
    }

    @Override
    protected Type getDefaultToType() {
        // In default, this ColumnGetter try to convert BigInteger value to Long value.
        // If value is too big for Long type, please choose Type.String or Type.JSON
        return Types.LONG;
    }

    @Override
    public void longColumn(Column column){
        try {

            to.setLong(column, Long.parseLong(value.toString()));

        }catch(NumberFormatException e){
            throw new NumberFormatException(
                    String.format(
                            "%s In '%s %s' is too large for Long type. \n" +
                            "Please set other type (string or json) in your config file : \n" +
                            "\n" +
                            "in:\n" +
                            "  type: clickhouse\n" +
                            "  ...\n" +
                            "  column_option:\n" +
                            "    %s: {type: string}\n",
                            value.toString(),
                            column.getName(),
                            originalColumnTypeName,
                            column.getName()
                    ));
        }
    }

    @Override
    public void stringColumn(Column column){
        to.setString(column, value.toString());
    }

    @Override
    public void jsonColumn(Column column){
        try {
            String jsonString = gson.toJson(value.toString());
            Value v = jsonParser.parse(jsonString);
            to.setJson(column, v);
        }
        catch (JsonParseException e) {
            super.jsonColumn(column);
        }
    }
}
