package org.embulk.input.postgresql.getter;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
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

import java.sql.Types;

public class ArrayColumnGetter
        extends AbstractColumnGetter
{
    protected Array value;

    protected final JsonParser jsonParser = new JsonParser();

    public ArrayColumnGetter(PageBuilder to, Type toType)
    {
        super(to, toType);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        value = from.getArray(fromIndex);
    }

    private ArrayNode buildJsonArray(Object[] elements)
            throws SQLException
    {
        ArrayNode arrayNode = jsonNodeFactory.arrayNode();
        for (Object v : elements) {
            if (v == null) {
                arrayNode.add(NullNode.getInstance());
                continue;
            }
            if (v.getClass().isArray()) {
                arrayNode.add(buildJsonArray((Object[]) v));
            }
            else {
                switch (value.getBaseType()) {
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        arrayNode.add(jsonNodeFactory.numberNode(((Number) v).longValue()));
                        break;
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        arrayNode.add(jsonNodeFactory.numberNode(((Number) v).doubleValue()));
                        break;
                    case Types.BOOLEAN:
                    case Types.BIT:  // JDBC BIT is boolean, unlike SQL-92
                        arrayNode.add(jsonNodeFactory.booleanNode((Boolean) v));
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CLOB:
                    case Types.NCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                        arrayNode.add(jsonNodeFactory.textNode((String) v));
                        break;
                }
            }
        }
        return arrayNode;
    }

    @Override
    protected Type getDefaultToType()
    {
        return org.embulk.spi.type.Types.STRING;
    }

    @Override
    public void jsonColumn(Column column)
    {
        try {
            Value v = jsonParser.parse(buildJsonArray((Object[]) value.getArray()).toString());
            to.setJson(column, v);
        }
        catch (JsonParseException | SQLException | ClassCastException e) {
            super.jsonColumn(column);
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        to.setString(column, value.toString());
    }
}
