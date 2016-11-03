package org.embulk.input.jdbc.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Type;

public abstract class AbstractTimestampColumnGetter
        extends AbstractColumnGetter
{
    protected final TimestampFormatter timestampFormatter;
    protected Timestamp value;

    public AbstractTimestampColumnGetter(PageBuilder to, Type toType, TimestampFormatter timestampFormatter)
    {
        super(to, toType);

        this.timestampFormatter = timestampFormatter;
    }

    @Override
    public void stringColumn(Column column)
    {
        to.setString(column, timestampFormatter.format(value));
    }

    @Override
    public void jsonColumn(Column column) {
        throw new UnsupportedOperationException("This plugin doesn't support json type. Please try to upgrade version of the plugin using 'embulk gem update' command. If the latest version still doesn't support json type, please contact plugin developers, or change configuration of input plugin not to use json type.");
    }

    @Override
    public void timestampColumn(Column column)
    {
        to.setTimestamp(column, value);
    }
}
