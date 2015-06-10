package org.embulk.input.oracle.getter;

import java.sql.Types;

import org.embulk.spi.PageBuilder;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.jdbc.getter.TimestampColumnGetter;

public class OracleColumnGetterFactory extends ColumnGetterFactory
{
    // TODO?
    //public static class HighPrecisionTimestampColumnGetter extends TimestampColumnGetter
    //{
    //    private final int fractionalSecondsPrecision;
    //
    //    public HighPrecisionTimestampColumnGetter(int fractionalSecondsPrecision)
    //    {
    //        if (fractionalSecondsPrecision <= 0) {
    //            throw new IllegalArgumentException("fractionalSecondsPrecision must be greater than 0");
    //        }
    //        this.fractionalSecondsPrecision = fractionalSecondsPrecision;
    //    }
    //
    //    @Override
    //    public Type getToType()
    //    {
    //        return Types.TIMESTAMP.withFormat("%Y-%m-%d %H:%M:%S.%" + fractionalSecondsPrecision + "N");
    //    }
    //}

    public OracleColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone)
    {
        super(to, defaultTimeZone);
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcColumn column, JdbcColumnOption option)
    {
        ColumnGetter getter = super.newColumnGetter(column);
        if (getter instanceof TimestampColumnGetter) {
            // TODO
            //return new HighPrecisionTimestampColumnGetter(column.getScale());
            return getter;
        } else {
            return getter;
        }
    }
}
