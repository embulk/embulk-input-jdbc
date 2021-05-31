package org.embulk.input.redshift.getter;

import java.time.ZoneId;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin.PluginTask;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.jdbc.getter.TimestampWithTimeZoneIncrementalHandler;
import org.embulk.input.jdbc.getter.TimestampWithoutTimeZoneIncrementalHandler;
import org.embulk.spi.PageBuilder;

public class RedshiftColumnGetterFactory extends ColumnGetterFactory
{
    public RedshiftColumnGetterFactory(final PageBuilder to, final ZoneId defaultTimeZone)
    {
        super(to, defaultTimeZone);
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcInputConnection con, PluginTask task, JdbcColumn column, JdbcColumnOption option)
    {
        ColumnGetter getter = super.newColumnGetter(con, task, column, option);

        // incremental loading wrapper
        switch (column.getTypeName()) {
        case "timestamptz":
            return new TimestampWithTimeZoneIncrementalHandler(getter);
        case "timestamp":
            return new TimestampWithoutTimeZoneIncrementalHandler(getter);
        default:
            return getter;
        }
    }
}
