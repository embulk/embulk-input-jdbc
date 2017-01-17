package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.xerial.snappy.Snappy;
import java.sql.Blob;
import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;

public class SnappyColumnGetter
        extends AbstractColumnGetter
{
    final JsonParser jsonParser = new JsonParser();

    private String value;

    public SnappyColumnGetter(PageBuilder to, Type toType)
    {
        super(to, toType);
    }

    private String json2String(String input) {
    	String result = "";
    	Gson gson = new Gson();
    	JSONArray e = (JSONArray)JsonPath.read(input, "$.*");
    	java.util.Iterator it = e.iterator();
    	while (it.hasNext()) {
    		String sss = gson.toJson(it.next());
    		if (sss.trim().startsWith("{")) {
    			result = result + sss + "||";
    		}
    		
    	}
    	if (result.equals("")) {
    		return "{}";
    	} else if (result.endsWith("||")) {
    		return result.substring(0, result.length() - 2);
    	} else {
    		return result;
    	}
    	
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        Blob blob = from.getBlob(fromIndex);
        try {
            String tmpValue = Snappy.uncompressString(blob.getBytes(1, (int) blob.length()));
            value = json2String(tmpValue);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
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

    @Override
    public JsonNode encodeToJson()
    {
        return jsonNodeFactory.textNode(value);
    }

    @Override
    public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
        throws SQLException
    {
        toStatement.setString(toIndex, fromValue.asText());
    }
}
