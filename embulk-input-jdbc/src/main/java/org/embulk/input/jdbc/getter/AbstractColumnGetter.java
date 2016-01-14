package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;

public abstract class AbstractColumnGetter implements ColumnGetter, ColumnVisitor
{
    protected final PageBuilder to;
    private final Type toType;

    public AbstractColumnGetter(PageBuilder to, Type toType)
    {
        this.to = to;
        this.toType = toType;
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
    public void jsonColumn(Column column) {
        throw new UnsupportedOperationException("This plugin doesn't support json type. Please try to upgrade version of the plugin using 'embulk gem update' command. If the latest version still doesn't support json type, please contact plugin developers, or change configuration of input plugin not to use json type.");
    }

    @Override
    public void timestampColumn(Column column)
    {
        to.setNull(column);
    }

    @Override
    public Type getToType()
    {
        if (toType == null) {
            return getDefaultToType();
        }
        return toType;
    }

    protected abstract Type getDefaultToType();

}
