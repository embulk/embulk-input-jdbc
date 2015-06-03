package org.embulk.input.jdbc.getter;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class ColumnGetters
{
    private ColumnGetters() { }

    public static class BooleanColumnGetter
            extends AbstractColumnGetter
    {
        public BooleanColumnGetter(PageBuilder to)
        {
            super(to);
        }

        @Override
        public void getAndSet(ResultSet from, int fromIndex,
                Column toColumn) throws SQLException
        {
            boolean v = from.getBoolean(fromIndex);
            if (from.wasNull()) {
                to.setNull(toColumn);
            } else {
                to.setBoolean(toColumn, v);
            }
        }

        @Override
        public Type getToType()
        {
            return Types.BOOLEAN;
        }
    }

    public static class LongColumnGetter
            extends AbstractColumnGetter
    {
        public LongColumnGetter(PageBuilder to)
        {
            super(to);
        }

        @Override
        public void getAndSet(ResultSet from, int fromIndex,
                Column toColumn) throws SQLException
        {
            long v = from.getLong(fromIndex);
            if (from.wasNull()) {
                to.setNull(toColumn);
            } else {
                to.setLong(toColumn, v);
            }
        }

        @Override
        public Type getToType()
        {
            return Types.LONG;
        }
    }

    public static class DoubleColumnGetter
            extends AbstractColumnGetter
    {
        public DoubleColumnGetter(PageBuilder to)
        {
            super(to);
        }

        @Override
        public void getAndSet(ResultSet from, int fromIndex,
                Column toColumn) throws SQLException
        {
            double v = from.getDouble(fromIndex);
            if (from.wasNull()) {
                to.setNull(toColumn);
            } else {
                to.setDouble(toColumn, v);
            }
        }

        @Override
        public Type getToType()
        {
            return Types.DOUBLE;
        }
    }

    public static class StringColumnGetter
            extends AbstractColumnGetter
    {
        public StringColumnGetter(PageBuilder to)
        {
            super(to);
        }

        @Override
        public void getAndSet(ResultSet from, int fromIndex,
                Column toColumn) throws SQLException
        {
            String v = from.getString(fromIndex);
            if (from.wasNull()) {
                to.setNull(toColumn);
            } else {
                to.setString(toColumn, v);
            }
        }

        @Override
        public Type getToType()
        {
            return Types.STRING;
        }
    }

    public static class DateColumnGetter
            extends AbstractColumnGetter
    {
        public DateColumnGetter(PageBuilder to)
        {
            super(to);
        }

        @Override
        public void getAndSet(ResultSet from, int fromIndex,
                Column toColumn) throws SQLException
        {
            java.sql.Date v = from.getDate(fromIndex);
            if (from.wasNull()) {
                to.setNull(toColumn);
            } else {
                Timestamp t = Timestamp.ofEpochMilli(v.getTime());
                to.setTimestamp(toColumn, t);
            }
        }

        @Override
        public Type getToType()
        {
            return Types.TIMESTAMP.withFormat("%Y-%m-%d");
        }
    }

    public static class TimeColumnGetter
            extends AbstractColumnGetter
    {
        public TimeColumnGetter(PageBuilder to)
        {
            super(to);
        }

        @Override
        public void getAndSet(ResultSet from, int fromIndex,
                Column toColumn) throws SQLException
        {
            java.sql.Time v = from.getTime(fromIndex);
            if (from.wasNull()) {
                to.setNull(toColumn);
            } else {
                Timestamp t = Timestamp.ofEpochMilli(v.getTime());
                to.setTimestamp(toColumn, t);
            }
        }

        @Override
        public Type getToType()
        {
            return Types.TIMESTAMP.withFormat("%H:%M:%S");
        }
    }

    public static class TimestampColumnGetter
            extends AbstractColumnGetter
    {
        public TimestampColumnGetter(PageBuilder to)
        {
            super(to);
        }

        @Override
        public void getAndSet(ResultSet from, int fromIndex,
                Column toColumn) throws SQLException
        {
            java.sql.Timestamp v = from.getTimestamp(fromIndex);
            if (from.wasNull()) {
                to.setNull(toColumn);
            } else {
                Timestamp t = Timestamp.ofEpochSecond(v.getTime() / 1000, v.getNanos());
                to.setTimestamp(toColumn, t);
            }
        }

        @Override
        public Type getToType()
        {
            return Types.TIMESTAMP.withFormat("%Y-%m-%d %H:%M:%S");
        }
    }

    public static class BigDecimalToDoubleColumnGetter
            extends AbstractColumnGetter
    {
        public BigDecimalToDoubleColumnGetter(PageBuilder to)
        {
            super(to);
        }

        @Override
        public void getAndSet(ResultSet from, int fromIndex,
                Column toColumn) throws SQLException
        {
            BigDecimal v = from.getBigDecimal(fromIndex);
            if (from.wasNull()) {
                to.setNull(toColumn);
            } else {
                // rounded value could be Double.NEGATIVE_INFINITY or Double.POSITIVE_INFINITY.
                double rounded = v.doubleValue();
                to.setDouble(toColumn, rounded);
            }
        }

        @Override
        public Type getToType()
        {
            return Types.DOUBLE;
        }
    }
}
