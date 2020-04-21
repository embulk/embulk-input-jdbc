package org.embulk.input.sqlserver.getter;

import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.*;
import org.embulk.spi.PageBuilder;

public class SQLServerColumnGetterFactory extends ColumnGetterFactory {

    public SQLServerColumnGetterFactory(final PageBuilder to, final String defaultTimeZone)
    {
        super(to, defaultTimeZone);
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcInputConnection con, AbstractJdbcInputPlugin.PluginTask task, JdbcColumn column, JdbcColumnOption option)
    {
        switch (column.getTypeName()) {
        case "date":
        case "datetime2":
        case "time":
        case "sql_variant":
        // DateTimeOffset is available only in MSSQL
        case "datetimeoffset":
            // because jTDS driver, default JDBC driver for older embulk-input-sqlserver, returns Types.VARCHAR as JDBC type for these types.
            return new StringColumnGetter(to,  getToType(option));

        case "datetime":
            ColumnGetter getter = super.newColumnGetter(con, task, column, option);
            return new TimestampWithoutTimeZoneIncrementalHandler(getter);

        default:
            return super.newColumnGetter(con, task, column, option);
        }
    }

}
