package org.embulk.input.jdbc.getter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.time.TimestampFormatter;
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

    private static interface FormatterIntlTask extends Task, TimestampFormatter.Task {}
    private static interface FormatterIntlColumnOption extends Task, TimestampFormatter.TimestampColumnOption {}

    @Override
    public JsonNode encodeToJson()
    {
        // TODO: Switch to a newer TimestampFormatter constructor after a reasonable interval.
        // Traditional constructor is used here for compatibility.
        final ConfigSource configSource = Exec.newConfigSource();
        configSource.set("format", ISO_USEC_FORMAT);
        configSource.set("timezone", "UTC");
        TimestampFormatter formatter = new TimestampFormatter(
            Exec.newConfigSource().loadConfig(FormatterIntlTask.class),
            Optional.fromNullable(configSource.loadConfig(FormatterIntlColumnOption.class)));
        String text = formatter.format(org.embulk.spi.time.Timestamp.ofEpochSecond(epochSecond, nano));
        return jsonNodeFactory.textNode(text);
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
        org.embulk.spi.time.Timestamp epoch = parser.parse(fromValue.asText());

        Timestamp sqlTimestamp = new Timestamp(epoch.getEpochSecond() * 1000);
        sqlTimestamp.setNanos(epoch.getNano());
        toStatement.setTimestamp(toIndex, sqlTimestamp);
    }
}
