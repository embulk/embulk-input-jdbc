package org.embulk.input.postgresql.getter;

import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
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
    public ColumnGetter newColumnGetter(JdbcColumn column, JdbcColumnOption option)
    {
        if (column.getTypeName().equals("hstore") && getToType(option) == Types.JSON) {
            return new HstoreToJsonColumnGetter(to, Types.JSON);
        } else {
            return super.newColumnGetter(column, option);
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
            // hstore is converted to string by default
            return "string";
        default:
            return super.sqlTypeToValueType(column, sqlType);
        }
    }
}
