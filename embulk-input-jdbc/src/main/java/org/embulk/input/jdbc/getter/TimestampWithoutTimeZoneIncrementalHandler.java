package org.embulk.input.jdbc.getter;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.embulk.config.ConfigException;
import org.embulk.spi.Column;
import static java.util.Locale.ENGLISH;

public class TimestampWithoutTimeZoneIncrementalHandler
        extends AbstractIncrementalHandler
{
    private static final String ISO_USEC_FORMAT = "%d-%02d-%02dT%02d:%02d:%02d.%06d";
    private static final Pattern ISO_USEC_PATTERN = Pattern.compile("(\\d+)-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2}):(\\d{2}).(\\d{6})");

    private Timestamp dateTime;

    public TimestampWithoutTimeZoneIncrementalHandler(ColumnGetter next)
    {
        super(next);
    }

    @Override
    public void getAndSet(ResultSet from, int fromIndex,
            Column toColumn) throws SQLException
    {
        // sniff the value
        Timestamp timestamp = from.getTimestamp(fromIndex);
        if (timestamp != null) {
            this.dateTime = timestamp;
        }

        super.getAndSet(from, fromIndex, toColumn);
    }

    @Override
    public JsonNode encodeToJson()
    {
        String text = String.format(ENGLISH,
                ISO_USEC_FORMAT,
                dateTime.getYear() + 1900,
                dateTime.getMonth() + 1,
                dateTime.getDate(),
                dateTime.getHours(),
                dateTime.getMinutes(),
                dateTime.getSeconds(),
                dateTime.getNanos() / 1000);
        return jsonNodeFactory.textNode(text);
    }

    @Override
    public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
        throws SQLException
    {
        Matcher matcher = ISO_USEC_PATTERN.matcher(fromValue.asText());
        if (!matcher.matches()) {
            throw new ConfigException("Invalid timestamp without time zone pattern: " + fromValue);
        }
        Timestamp sqlDateTime = new Timestamp(
                Integer.parseInt(matcher.group(1)) - 1900,  // year
                Integer.parseInt(matcher.group(2)) - 1,  // month
                Integer.parseInt(matcher.group(3)),  // day
                Integer.parseInt(matcher.group(4)),  // hour
                Integer.parseInt(matcher.group(5)),  // minute
                Integer.parseInt(matcher.group(6)),  // second
                Integer.parseInt(matcher.group(7)) * 1000);  // usec -> nsec
        toStatement.setTimestamp(toIndex, sqlDateTime);
    }
}
