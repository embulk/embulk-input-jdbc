package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class TimestampColumnGetter
        extends AbstractTimestampColumnGetter
{
    static final String DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S";

    public TimestampColumnGetter(PageBuilder to, Type toType, TimestampFormatter timestampFormatter)
    {
        super(to, toType, timestampFormatter);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        java.sql.Timestamp timestamp = from.getTimestamp(fromIndex);
        if (timestamp != null) {
            value = Timestamp.ofEpochSecond(timestamp.getTime() / 1000, timestamp.getNanos());
        }
    }

    @Override
    protected Type getDefaultToType()
    {
        return Types.TIMESTAMP.withFormat(DEFAULT_FORMAT);
    }

}
