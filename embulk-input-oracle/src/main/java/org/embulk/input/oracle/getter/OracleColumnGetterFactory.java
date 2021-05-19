package org.embulk.input.oracle.getter;

import java.time.ZoneId;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.jdbc.getter.TimestampWithoutTimeZoneIncrementalHandler;
import org.embulk.spi.PageBuilder;

public class OracleColumnGetterFactory extends ColumnGetterFactory
{

  public OracleColumnGetterFactory(final PageBuilder to, final ZoneId defaultTimeZone) {
    super(to, defaultTimeZone);
  }

  @Override
  public ColumnGetter newColumnGetter(JdbcInputConnection con, AbstractJdbcInputPlugin.PluginTask task, JdbcColumn column, JdbcColumnOption option)
  {

    ColumnGetter getter = super.newColumnGetter(con, task, column, option);

    switch (column.getTypeName()) {
      case "DATE":
        return new TimestampWithoutTimeZoneIncrementalHandler(getter);
      case "TIMESTAMP":
        return new TimestampWithoutTimeZoneIncrementalHandler(getter);
      default:
        return getter;
    }
  }

}
