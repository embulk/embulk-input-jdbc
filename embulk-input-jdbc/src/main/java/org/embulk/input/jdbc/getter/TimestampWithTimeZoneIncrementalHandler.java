package org.embulk.input.jdbc.getter;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.time.TimestampFormatter.FormatterTask;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser.ParserTask;
import org.embulk.spi.time.TimestampParser;

public class TimestampWithTimeZoneIncrementalHandler
        extends AbstractIncrementalHandler
{
    private static final String ISO_USEC_FORMAT = "%Y-%m-%dT%H:%M:%S.%6NZ";
    private static final String ISO_USEC_PATTERN = "%Y-%m-%dT%H:%M:%S.%N%z";

    private long epochSecond;
    private int nano;

    public TimestampWithTimeZoneIncrementalHandler(ColumnGetter next)
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
            epochSecond = timestamp.getTime() / 1000;
            nano = timestamp.getNanos();
        }

        super.getAndSet(from, fromIndex, toColumn);
    }

    @Override
    public JsonNode encodeToJson()
    {
        FormatterTask task = Exec.newConfigSource()
            .set("timezone", "UTC")
            .loadConfig(FormatterTask.class);
        TimestampFormatter formatter = new TimestampFormatter(ISO_USEC_FORMAT, task);
        String text = formatter.format(org.embulk.spi.time.Timestamp.ofEpochSecond(epochSecond, nano));
        return jsonNodeFactory.textNode(text);
    }

    @Override
    public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
        throws SQLException
    {
        ParserTask task = Exec.newConfigSource()
            .set("default_timezone", "UTC")
            .loadConfig(ParserTask.class);
        TimestampParser parser = new TimestampParser(ISO_USEC_PATTERN, task);
        org.embulk.spi.time.Timestamp epoch = parser.parse(fromValue.asText());

        Timestamp sqlTimestamp = new Timestamp(epoch.getEpochSecond() * 1000);
        sqlTimestamp.setNanos(epoch.getNano());
        toStatement.setTimestamp(toIndex, sqlTimestamp);
    }
}
