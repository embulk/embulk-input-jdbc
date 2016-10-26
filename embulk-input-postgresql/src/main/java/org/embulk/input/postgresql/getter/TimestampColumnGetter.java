package org.embulk.input.postgresql.getter;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.config.ConfigException;
import org.embulk.input.jdbc.getter.AbstractTimestampColumnGetter;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.joda.time.DateTimeZone;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TimestampColumnGetter
        extends AbstractTimestampColumnGetter
{
    private static final String DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S";
    private final TimestampFormatter formatter;
    private final String columnTypeName;
    private final DateTimeZone timezone;

    public TimestampColumnGetter(PageBuilder to, Type toType, String columnTypeName, TimestampFormatter timestampFormatter, DateTimeZone timezone)
    {
        super(to, toType, timestampFormatter);
        this.formatter = timestampFormatter;
        this.columnTypeName = columnTypeName;
        this.timezone = timezone;
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        java.sql.Timestamp timestamp = from.getTimestamp(fromIndex);
        if (timestamp != null) {
            value = Timestamp.ofEpochSecond(timestamp.getTime() / 1000, timestamp.getNanos());
        }
    }

    @Override
    protected Type getDefaultToType()
    {
        return Types.TIMESTAMP.withFormat(DEFAULT_FORMAT);
    }

    @Override
    public JsonNode encodeToJson()
    {
        return jsonNodeFactory.textNode(formatter.format(value));
    }

    @Override
    public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
            throws SQLException
    {
        switch (columnTypeName) {
            case "timestamp":
                toStatement.setTimestamp(toIndex, java.sql.Timestamp.valueOf(fromValue.asText()));
                break;
            case "timestamptz":
                SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS z");
                dateFormatter.setTimeZone(timezone.toTimeZone());
                try {
                    java.sql.Timestamp t = new java.sql.Timestamp(dateFormatter.parse(fromValue.asText()).getTime());
                    toStatement.setTimestamp(toIndex, t);
                } catch (ParseException e) {
                    throw new ConfigException(e);
                }
                break;
            default:
                toStatement.setTimestamp(toIndex, java.sql.Timestamp.valueOf(fromValue.asText()));
        }
    }
}
