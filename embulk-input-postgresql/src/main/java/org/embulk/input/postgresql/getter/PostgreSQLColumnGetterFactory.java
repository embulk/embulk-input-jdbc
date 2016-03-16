package org.embulk.input.postgresql.getter;

import com.google.common.base.Optional;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.ToStringMap;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

public class PostgreSQLColumnGetterFactory extends ColumnGetterFactory
{
    public PostgreSQLColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone, Optional<JdbcColumnOption> convertDateToString)
    {
        super(to, defaultTimeZone, convertDateToString);
    }

    @Override
    protected String sqlTypeToValueType(JdbcColumn column, int sqlType)
    {
        if (column.getTypeName().equals("json") || column.getTypeName().equals("jsonb")) {
            return "json";
        } else {
            return super.sqlTypeToValueType(column, sqlType);
        }
    }
}
