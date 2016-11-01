package org.embulk.input.mysql.getter;

import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampFormatter;
import org.joda.time.DateTimeZone;

public class MySQLColumnGetterFactory extends ColumnGetterFactory
{
    private final DateTimeZone defaultTimeZone;
    private static final String DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S.%6N";

    public MySQLColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone)
    {
        super(to, defaultTimeZone);
        this.defaultTimeZone = defaultTimeZone;
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcColumn column, JdbcColumnOption option)
    {
        switch(column.getTypeName()) {
            case "DATETIME":
            case "TIMESTAMP":
                return new TimestampColumnGetter(to, getToType(option), newTimestampFormatter(option, DEFAULT_FORMAT));
            default:
                return super.newColumnGetter(column, option);
        }
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

    private TimestampFormatter newTimestampFormatter(JdbcColumnOption option, String defaultTimestampFormat)
    {
        return new TimestampFormatter(
                option.getJRuby(),
                option.getTimestampFormat().isPresent() ? option.getTimestampFormat().get().getFormat() : defaultTimestampFormat,
                option.getTimeZone().or(defaultTimeZone));
    }
}
