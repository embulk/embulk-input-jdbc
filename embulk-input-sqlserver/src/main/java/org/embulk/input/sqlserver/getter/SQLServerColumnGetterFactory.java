package org.embulk.input.sqlserver.getter;

import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.*;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

public class SQLServerColumnGetterFactory extends ColumnGetterFactory {

    public SQLServerColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone)
    {
        super(to, defaultTimeZone);
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcInputConnection con, AbstractJdbcInputPlugin.PluginTask task, JdbcColumn column, JdbcColumnOption option)
    {
        ColumnGetter getter = super.newColumnGetter(con, task, column, option);
        switch (column.getTypeName()) {
            case "datetime":
                return new TimestampWithoutTimeZoneIncrementalHandler(getter);
            default:
                return getter;
        }
    }

}