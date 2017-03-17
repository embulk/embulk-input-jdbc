package org.embulk.input.oracle.getter;

import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.jdbc.getter.TimestampWithoutTimeZoneIncrementalHandler;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

public class OracleColumnGetterFactory extends ColumnGetterFactory
{

  public OracleColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone) {
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
