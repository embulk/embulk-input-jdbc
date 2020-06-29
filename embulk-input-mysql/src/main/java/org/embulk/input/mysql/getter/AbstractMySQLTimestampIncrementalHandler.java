package org.embulk.input.mysql.getter;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.input.jdbc.getter.AbstractIncrementalHandler;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;
import org.joda.time.DateTimeZone;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;

public abstract class AbstractMySQLTimestampIncrementalHandler
        extends AbstractIncrementalHandler
{
    protected final DateTimeZone sessionTimeZone;
    protected long epochSecond;
    protected int nano;

    public AbstractMySQLTimestampIncrementalHandler(DateTimeZone sessionTimeZone, ColumnGetter next)
    {
        super(next);
        this.sessionTimeZone = sessionTimeZone;
    }

    @Override
    public void getAndSet(ResultSet from, int fromIndex, Column toColumn)
            throws SQLException
    {
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
        configSource.set("format", getTimestampFormat());
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
        String text = formatter.format(utcTimestampFromSessionTime(epochSecond, nano));
        return jsonNodeFactory.textNode(text);
    }

    protected abstract String getTimestampFormat();

    protected abstract org.embulk.spi.time.Timestamp utcTimestampFromSessionTime(long epochSecond, int nano);

    private static interface ParserIntlTask extends Task, TimestampParser.Task {}
    private static interface ParserIntlColumnOption extends Task, TimestampParser.TimestampColumnOption {}

    @Override
    public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
            throws SQLException
    {
        // TODO: Switch to a newer TimestampParser constructor after a reasonable interval.
        // Traditional constructor is used here for compatibility.
        final ConfigSource configSource = Exec.newConfigSource();
        configSource.set("format", getTimestampPattern());
        configSource.set("timezone", "UTC");
        TimestampParser parser = new TimestampParser(
            Exec.newConfigSource().loadConfig(ParserIntlTask.class),
            configSource.loadConfig(ParserIntlColumnOption.class));
        org.embulk.spi.time.Timestamp epoch = parser.parse(fromValue.asText());
        toStatement.setTimestamp(toIndex, utcTimestampToSessionTime(epoch));
    }

    protected abstract String getTimestampPattern();

    protected abstract Timestamp utcTimestampToSessionTime(org.embulk.spi.time.Timestamp ts);
}
