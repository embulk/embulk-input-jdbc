package org.embulk.input.jdbc.getter;

import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import com.google.common.math.DoubleMath;

public class FloatColumnGetter
        extends AbstractColumnGetter
{
    protected float value;

    public FloatColumnGetter(PageBuilder to, Type toType)
    {
        super(to, toType);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        value = from.getFloat(fromIndex);
    }

    @Override
    protected Type getDefaultToType()
    {
        return Types.DOUBLE;
    }

    @Override
    public void booleanColumn(Column column)
    {
        to.setBoolean(column, value > 0.0);
    }

    @Override
    public void longColumn(Column column)
    {
        long l;
        try {
            // TODO configurable rounding mode
            l = DoubleMath.roundToLong(value, RoundingMode.HALF_UP);
        } catch (ArithmeticException e) {
            // NaN / Infinite / -Infinite
            super.longColumn(column);
            return;
        }
        to.setLong(column, l);
    }

    @Override
    public void doubleColumn(Column column)
    {
        to.setDouble(column, value);
    }

    @Override
    public void stringColumn(Column column)
    {
        to.setString(column, Float.toString(value));
    }
}
