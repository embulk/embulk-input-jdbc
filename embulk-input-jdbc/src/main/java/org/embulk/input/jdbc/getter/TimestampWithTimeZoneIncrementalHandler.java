package org.embulk.input.jdbc.getter;

import com.fasterxml.jackson.databind.JsonNode;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.time.TimestampParser;
import org.embulk.util.timestamp.TimestampFormatter;

public class TimestampWithTimeZoneIncrementalHandler
        extends AbstractIncrementalHandler
{
    // maybe "...%6N%Z", but shouldn't correct for compatibility.
    private static final TimestampFormatter FORMATTER = TimestampFormatter.builderWithRuby("%Y-%m-%dT%H:%M:%S.%6NZ").build();

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
        return jsonNodeFactory.textNode(format(epochSecond, nano));
    }

    private String format(long epochSecond, int nano)
    {
        return FORMATTER.format(Instant.ofEpochSecond(epochSecond, nano));
    }

    private static interface ParserIntlTask extends Task, TimestampParser.Task {}
    private static interface ParserIntlColumnOption extends Task, TimestampParser.TimestampColumnOption {}

    @Override
    public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
        throws SQLException
    {
        // TODO: Switch to a newer TimestampParser constructor after a reasonable interval.
        // Traditional constructor is used here for compatibility.
        final ConfigSource configSource = Exec.newConfigSource();
        configSource.set("format", ISO_USEC_PATTERN);
        configSource.set("timezone", "UTC");
        TimestampParser parser = new TimestampParser(
            Exec.newConfigSource().loadConfig(ParserIntlTask.class),
            configSource.loadConfig(ParserIntlColumnOption.class));

        try {
            org.embulk.spi.time.Timestamp epoch = parser.parse(fromValue.asText());
            Timestamp sqlTimestamp = new Timestamp(epoch.getEpochSecond() * 1000);
            sqlTimestamp.setNanos(epoch.getNano());
            toStatement.setTimestamp(toIndex, sqlTimestamp);

        } catch (TimestampParseException e) {
            long now = System.currentTimeMillis();
            String sample = format(now / 1000, (int)((now % 1000)*1000000));
            throw new ConfigException("Invalid timestamp with time zone pattern: " + fromValue + "."
                    + " The pattern must be 'yyyy-MM-ddTHH:mm:ss.SSSSSSZ'."
                    + " e.g. \"" + sample + "\" or \"" + sample.replace("Z", "+0000") + "\"");
        }

    }
}
