package org.embulk.input.oracle.getter;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.input.jdbc.getter.AbstractIncrementalHandler;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class OracleTimestampWithTimeZoneIncrementalHandler extends AbstractIncrementalHandler
{
  private static final String ISO_USEC_FORMAT = "%Y-%m-%d %H:%M:%S.%N %z";
  private static final String ISO_USEC_PATTERN = "%Y-%m-%d %H:%M:%S.%N %z";

  private long epochSecond;
  private int nano;

  public OracleTimestampWithTimeZoneIncrementalHandler(ColumnGetter next)
  {
    super(next);
  }

  @Override
  public void getAndSet(ResultSet from, int fromIndex,
                        Column toColumn) throws SQLException
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
    TimestampFormatter.FormatterTask task = Exec.newConfigSource()
            .set("timezone", "UTC")
            .loadConfig(TimestampFormatter.FormatterTask.class);
    TimestampFormatter formatter = new TimestampFormatter(ISO_USEC_FORMAT, task);
    String text = formatter.format(org.embulk.spi.time.Timestamp.ofEpochSecond(epochSecond, nano));
    return jsonNodeFactory.textNode(text);
  }

  @Override
  public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
          throws SQLException
  {
    TimestampParser.ParserTask task = Exec.newConfigSource()
            .set("default_timezone", "UTC")
            .loadConfig(TimestampParser.ParserTask.class);
    TimestampParser parser = new TimestampParser(ISO_USEC_PATTERN, task);
    org.embulk.spi.time.Timestamp epoch = parser.parse(fromValue.asText());

    Timestamp sqlTimestamp = new Timestamp(epoch.getEpochSecond() * 1000);
    sqlTimestamp.setNanos(epoch.getNano());
    toStatement.setTimestamp(toIndex, sqlTimestamp);
  }
}
