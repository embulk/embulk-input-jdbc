package org.embulk.input.postgresql.getter;

import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;
import org.joda.time.DateTimeZone;

public class PostgreSQLColumnGetterFactory extends ColumnGetterFactory
{
    private final DateTimeZone defaultTimeZone;
    private static final String TIMESTAMP_DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S.%6N";
    private static final String TIMESTAMPTZ_DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S.%6N %Z";

    public PostgreSQLColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone)
    {
        super(to, defaultTimeZone);
        this.defaultTimeZone = defaultTimeZone;
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcColumn column, JdbcColumnOption option)
    {
        String columnTypeName = column.getTypeName().toLowerCase();
        switch(columnTypeName) {
            case "hstore":
                return new HstoreColumnGetter(to, getToType(option));
            case "timestamp":
                return new TimestampColumnGetter(to, getToType(option), columnTypeName,
                        newTimestampFormatter(option, TIMESTAMP_DEFAULT_FORMAT), newTimestampParser(option, TIMESTAMP_DEFAULT_FORMAT));
            case "timestamptz":
                return new TimestampColumnGetter(to, getToType(option), columnTypeName,
                        newTimestampFormatter(option, TIMESTAMPTZ_DEFAULT_FORMAT), newTimestampParser(option, TIMESTAMPTZ_DEFAULT_FORMAT));
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

    private TimestampParser newTimestampParser(JdbcColumnOption option, String defaultTimestampFormat)
    {
        return new TimestampParser(
                option.getJRuby(),
                option.getTimestampFormat().isPresent() ? option.getTimestampFormat().get().getFormat() : defaultTimestampFormat,
                option.getTimeZone().or(defaultTimeZone));
    }
}
