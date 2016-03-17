package org.embulk.input.postgresql.getter;

import com.google.common.base.Optional;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

import java.util.Map;

public class PostgreSQLColumnGetterFactory extends ColumnGetterFactory
{
    public PostgreSQLColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone, Map<String, JdbcColumnOption> convertTypesToString)
    {
        super(to, defaultTimeZone, convertTypesToString);
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
