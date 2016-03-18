package org.embulk.input.jdbc.getter;

import java.sql.Types;
import java.util.Map;

import org.embulk.config.ConfigException;
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
    private final Map<String, JdbcColumnOption> defaultColumnOptions;

    public ColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone, Map<String, JdbcColumnOption> defaultColumnOptions)
    {
        this.to = to;
        this.defaultTimeZone = defaultTimeZone;
        this.defaultColumnOptions = defaultColumnOptions;
    }

    public ColumnGetter newColumnGetter(JdbcColumn column, JdbcColumnOption option)
    {
        return newColumnGetter(column, option, option.getValueType());
    }

    private ColumnGetter newColumnGetter(JdbcColumn column, JdbcColumnOption option, String valueType)
    {
        Type toType = getToType(option);
        switch(valueType) {
        case "coalesce":
            return newColumnGetter(column, option, sqlTypeToValueType(column, column.getSqlType()));
        case "long":
            return new LongColumnGetter(to, toType);
        case "float":
            return new FloatColumnGetter(to, toType);
        case "double":
            return new DoubleColumnGetter(to, toType);
        case "boolean":
            return new BooleanColumnGetter(to, toType);
        case "string":
            return new StringColumnGetter(to, toType);
        case "json":
            return new JsonColumnGetter(to, toType);
        case "date":
            return new DateColumnGetter(to, toType, newTimestampFormatter(option, DateColumnGetter.DEFAULT_FORMAT));
        case "time":
            return new TimeColumnGetter(to, toType, newTimestampFormatter(option, DateColumnGetter.DEFAULT_FORMAT));
        case "timestamp":
            return new TimestampColumnGetter(to, toType, newTimestampFormatter(option, DateColumnGetter.DEFAULT_FORMAT));
        case "decimal":
            return new BigDecimalColumnGetter(to, toType);
        default:
            throw new ConfigException(String.format("Unknown value_type '%s' for column '%s'", option.getValueType(), column.getName()));
        }
    }

    public String sqlTypeToValueType(JdbcColumn column)
    {
        return sqlTypeToValueType(column, column.getSqlType());
    }

    protected String sqlTypeToValueType(JdbcColumn column, int sqlType)
    {
        switch(sqlType) {
        // getLong
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
            return "long";

        // getFloat
        case Types.FLOAT:
        case Types.REAL:
            return "float";

        // getDouble
        case Types.DOUBLE:
            return "double";

        // getBool
        case Types.BOOLEAN:
        case Types.BIT:  // JDBC BIT is boolean, unlike SQL-92
            return "boolean";

        // getString, Clob
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.CLOB:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            return "string";

        // TODO
        //// getBytes Blob
        //case Types.BINARY:
        //case Types.VARBINARY:
        //case Types.LONGVARBINARY:
        //case Types.BLOB:
        //    return new BytesColumnGetter();

        // getDate
        case Types.DATE:
            return "date";

        // getTime
        case Types.TIME:
            return "time";

        // getTimestamp
        case Types.TIMESTAMP:
            return "timestamp";

        // TODO
        //// Null
        //case Types.NULL:
        //    return new NullColumnGetter();

        // getBigDecimal
        case Types.NUMERIC:
        case Types.DECIMAL:
            return "decimal";

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
