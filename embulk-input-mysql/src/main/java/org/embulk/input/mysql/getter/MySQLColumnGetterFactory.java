package org.embulk.input.mysql.getter;

import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.jdbc.getter.TimestampWithTimeZoneIncrementalHandler;
import org.embulk.input.jdbc.getter.TimestampWithoutTimeZoneIncrementalHandler;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

public class MySQLColumnGetterFactory extends ColumnGetterFactory
{
    public MySQLColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone)
    {
        super(to, defaultTimeZone);
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcColumn column, JdbcColumnOption option)
    {
        ColumnGetter getter = super.newColumnGetter(column, option);

        // incremental loading wrapper
        switch (column.getTypeName()) {
        case "DATETIME":
            return new TimestampWithoutTimeZoneIncrementalHandler(getter);
        case "TIMESTAMP":
            return new TimestampWithTimeZoneIncrementalHandler(getter);
        default:
            return getter;
        }
    }
}
