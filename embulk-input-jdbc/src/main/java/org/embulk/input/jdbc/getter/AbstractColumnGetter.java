package org.embulk.input.jdbc.getter;

import org.embulk.spi.PageBuilder;

public abstract class AbstractColumnGetter implements ColumnGetter
{
    protected final PageBuilder to;

    public AbstractColumnGetter(PageBuilder to)
    {
        this.to = to;
    }

}
