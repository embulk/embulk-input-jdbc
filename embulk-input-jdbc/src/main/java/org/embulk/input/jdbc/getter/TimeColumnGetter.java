package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class TimeColumnGetter
        extends AbstractTimestampColumnGetter
{
    static final String DEFAULT_FORMAT = "%H:%M:%S";

    public TimeColumnGetter(PageBuilder to, Type toType, TimestampFormatter timestampFormatter)
    {
        super(to, toType, timestampFormatter);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        Time time = from.getTime(fromIndex);
        if (time != null) {
            value = Timestamp.ofEpochMilli(time.getTime());
        }
    }

    @Override
    protected Type getDefaultToType()
    {
        return Types.TIMESTAMP.withFormat(DEFAULT_FORMAT);
    }

}
