package org.embulk.input.jdbc.getter;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class DateColumnGetter
        extends AbstractTimestampColumnGetter
{
    static final String DEFAULT_FORMAT = "%Y-%m-%d";

    public DateColumnGetter(PageBuilder to, Type toType, TimestampFormatter timestampFormatter)
    {
        super(to, toType, timestampFormatter);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex) throws SQLException
    {
        Date date = from.getDate(fromIndex);
        if (date != null) {
            value = Timestamp.ofEpochMilli(date.getTime());
        }
    }

    @Override
    protected Type getDefaultToType()
    {
        return Types.TIMESTAMP.withFormat(DEFAULT_FORMAT);
    }

}
