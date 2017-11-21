package org.embulk.input.postgresql.getter;

import org.embulk.input.jdbc.AbstractJdbcInputPlugin.PluginTask;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.jdbc.getter.TimestampWithTimeZoneIncrementalHandler;
import org.embulk.input.jdbc.getter.TimestampWithoutTimeZoneIncrementalHandler;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Types;
import org.joda.time.DateTimeZone;

public class PostgreSQLColumnGetterFactory extends ColumnGetterFactory
{
    public PostgreSQLColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone)
    {
        super(to, defaultTimeZone);
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcInputConnection con, PluginTask task, JdbcColumn column, JdbcColumnOption option)
    {
        if (column.getTypeName().equals("hstore") && getToType(option) == Types.JSON) {
            // converting hstore to json needs a special handling
            return new HstoreToJsonColumnGetter(to, Types.JSON);
        }

        if (column.getSqlType() == java.sql.Types.ARRAY) {
            return new ArrayColumnGetter(to, getToType(option));
        }

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

    @Override
    protected String sqlTypeToValueType(JdbcColumn column, int sqlType)
    {
        switch(column.getTypeName()) {
        case "json":
        case "jsonb":
            return "json";
        case "hstore":
        case "array":
            // array & hstore is converted to string by default
            return "string";
        default:
            return super.sqlTypeToValueType(column, sqlType);
        }
    }
}
