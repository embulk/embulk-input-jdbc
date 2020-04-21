package org.embulk.input.jdbc.getter;

import com.fasterxml.jackson.databind.JsonNode;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;

import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.time.TimestampParser;

public class TimestampWithTimeZoneIncrementalHandler
        extends AbstractIncrementalHandler
{
    private static final String ISO_USEC_FORMAT = "%Y-%m-%dT%H:%M:%S.%6NZ"; // maybe "...%6N%Z", but shouldn't correct for compatibility.
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

    private static interface FormatterIntlTask extends Task, TimestampFormatter.Task {}
    private static interface FormatterIntlColumnOption extends Task, TimestampFormatter.TimestampColumnOption {}

    @Override
    public JsonNode encodeToJson()
    {
        return jsonNodeFactory.textNode(format(epochSecond, nano));
    }

    private String format(long epochSecond, int nano)
    {
        // TODO: Switch to a newer TimestampFormatter constructor after a reasonable interval.
        // Traditional constructor is used here for compatibility.
        final ConfigSource configSource = Exec.newConfigSource();
        configSource.set("format", ISO_USEC_FORMAT);
        configSource.set("timezone", "UTC");
        final FormatterIntlTask task = Exec.newConfigSource().loadConfig(FormatterIntlTask.class);
        final Optional<? extends TimestampFormatter.TimestampColumnOption> columnOption =
                Optional.ofNullable(configSource.loadConfig(FormatterIntlColumnOption.class));
        final TimestampFormatter formatter = TimestampFormatter.of(
                columnOption.isPresent()
                        ? columnOption.get().getFormat().or(task.getDefaultTimestampFormat())
                        : task.getDefaultTimestampFormat(),
                columnOption.isPresent()
                        ? columnOption.get().getTimeZoneId().or(task.getDefaultTimeZoneId())
                        : task.getDefaultTimeZoneId());

        return formatter.format(org.embulk.spi.time.Timestamp.ofEpochSecond(epochSecond, nano));
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
