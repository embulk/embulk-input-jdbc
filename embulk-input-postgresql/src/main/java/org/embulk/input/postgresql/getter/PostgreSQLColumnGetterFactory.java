package org.embulk.input.postgresql.getter;

import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Types;
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
        if (columnTypeName.equals("hstore") && getToType(option) == Types.JSON) {
            // converting hstore to json needs special handling
            return new HstoreToJsonColumnGetter(to, Types.JSON);
        }
        return super.newColumnGetter(column, option);
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

    private TimestampFormatter newTimestampFormatter(JdbcColumnOption option, String defaultTimestampFormat)
    {
        return new TimestampFormatter(
                option.getJRuby(),
                option.getTimestampFormat().isPresent() ? option.getTimestampFormat().get().getFormat() : defaultTimestampFormat,
                option.getTimeZone().or(defaultTimeZone));
    }
}
