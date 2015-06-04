package org.embulk.input.jdbc.getter;

import java.sql.Types;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;

public class ColumnGetterFactory
{
    private final PageBuilder to;
    private final DateTimeZone defaultTimeZone;

    public ColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone)
    {
        this.to = to;
        this.defaultTimeZone = defaultTimeZone;
    }

    public ColumnGetter newColumnGetter(JdbcColumn column, JdbcColumnOption option)
    {
        Type toType = getToType(option);
        switch(column.getSqlType()) {
        // getLong
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
            return new LongColumnGetter(to, toType);

        // getFloat
        case Types.FLOAT:
        case Types.REAL:
            return new FloatColumnGetter(to, toType);

        // getDouble
        case Types.DOUBLE:
            return new DoubleColumnGetter(to, toType);

        // getBool
        case Types.BOOLEAN:
        case Types.BIT:  // JDBC BIT is boolean, unlike SQL-92
            return new BooleanColumnGetter(to, toType);

        // getString, Clob
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.CLOB:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            return new StringColumnGetter(to, toType);

        // TODO
        //// getBytes Blob
        //case Types.BINARY:
        //case Types.VARBINARY:
        //case Types.LONGVARBINARY:
        //case Types.BLOB:
        //    return new BytesColumnGetter();

        // getDate
        case Types.DATE:
            return new DateColumnGetter(to, toType, newTimestampFormatter(option, DateColumnGetter.DEFAULT_FORMAT));

        // getTime
        case Types.TIME:
            return new TimeColumnGetter(to, toType, newTimestampFormatter(option, TimeColumnGetter.DEFAULT_FORMAT));

        // getTimestamp
        case Types.TIMESTAMP:
            return new TimestampColumnGetter(to, toType, newTimestampFormatter(option, TimestampColumnGetter.DEFAULT_FORMAT));

        // TODO
        //// Null
        //case Types.NULL:
        //    return new NullColumnGetter();

        // getBigDecimal
        case Types.NUMERIC:
        case Types.DECIMAL:
            return new BigDecimalColumnGetter(to, toType);

        // others
        case Types.ARRAY:  // array
        case Types.STRUCT: // map
        case Types.REF:
        case Types.DATALINK:
        case Types.SQLXML: // XML
        case Types.ROWID:
        case Types.DISTINCT:
        case Types.JAVA_OBJECT:
        case Types.OTHER:
        default:
            throw unsupportedOperationException(column);
        }
    }

    private Type getToType(JdbcColumnOption option)
    {
        if (!option.getType().isPresent()) {
            return null;
        }
        Type toType = option.getType().get();
        if (toType instanceof TimestampType && option.getTimestampFormat().isPresent()) {
            toType = ((TimestampType)toType).withFormat(option.getTimestampFormat().get().getFormat());
        }
        return toType;
    }

    private TimestampFormatter newTimestampFormatter(JdbcColumnOption option, String defaultTimestampFormat)
    {
        return new TimestampFormatter(
                option.getJRuby(),
                option.getTimestampFormat().isPresent() ? option.getTimestampFormat().get().getFormat() : defaultTimestampFormat,
                option.getTimeZone().or(defaultTimeZone));
    }

    private static UnsupportedOperationException unsupportedOperationException(JdbcColumn column)
    {
        throw new UnsupportedOperationException(
                String.format("Unsupported type %s (sqlType=%d) of '%s' column. Please exclude the column from 'select:' option.",
                    column.getTypeName(), column.getSqlType(), column.getName()));
    }
}
