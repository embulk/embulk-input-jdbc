package org.embulk.input.mysql.getter;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.input.jdbc.getter.AbstractIncrementalHandler;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampFormatter.FormatterTask;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.time.TimestampParser.ParserTask;
import org.joda.time.DateTimeZone;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public abstract class AbstractMySQLIncrementalHandler
        extends AbstractIncrementalHandler
{
    protected final DateTimeZone sessionTimeZone;
    protected long epochSecond;
    protected int nano;

    public AbstractMySQLIncrementalHandler(DateTimeZone sessionTimeZone, ColumnGetter next)
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

    @Override
    public JsonNode encodeToJson()
    {
        FormatterTask task = Exec.newConfigSource()
            .set("timezone", "UTC")
            .loadConfig(FormatterTask.class);
        TimestampFormatter formatter = new TimestampFormatter(getTimestampFormat(), task);
        String text = formatter.format(convertTimestamp(epochSecond, nano));
        return jsonNodeFactory.textNode(text);
    }

    protected abstract String getTimestampFormat();

    protected abstract org.embulk.spi.time.Timestamp convertTimestamp(long epochSecond, int nano);

    @Override
    public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
            throws SQLException
    {
        ParserTask task = Exec.newConfigSource()
            .set("default_timezone", "UTC")
            .loadConfig(ParserTask.class);
        TimestampParser parser = new TimestampParser(getTimestampPattern(), task);
        org.embulk.spi.time.Timestamp epoch = parser.parse(fromValue.asText());
        toStatement.setTimestamp(toIndex, convertTimestamp(epoch));
    }

    protected abstract String getTimestampPattern();

    protected abstract Timestamp convertTimestamp(org.embulk.spi.time.Timestamp ts);
}
