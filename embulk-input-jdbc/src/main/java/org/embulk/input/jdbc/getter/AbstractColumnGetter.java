package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;

public abstract class AbstractColumnGetter implements ColumnGetter, ColumnVisitor
{
    protected final PageBuilder to;

    public AbstractColumnGetter(PageBuilder to)
    {
        this.to = to;
    }

    @Override
    public void getAndSet(ResultSet from, int fromIndex,
            Column toColumn) throws SQLException {

        fetch(from, fromIndex);
        if (from.wasNull()) {
            to.setNull(toColumn);
        } else {
            toColumn.visit(this);
        }
    }

    protected abstract void fetch(ResultSet from, int fromIndex) throws SQLException;

    @Override
    public void booleanColumn(Column column)
    {
        to.setNull(column);
    }

    @Override
    public void longColumn(Column column)
    {
        to.setNull(column);
    }

    @Override
    public void doubleColumn(Column column)
    {
        to.setNull(column);
    }

    @Override
    public void stringColumn(Column column)
    {
        to.setNull(column);
    }

    @Override
    public void timestampColumn(Column column)
    {
        to.setNull(column);
    }

}
