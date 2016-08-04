package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.DataException;
import static java.util.Locale.ENGLISH;

public abstract class AbstractColumnGetter implements ColumnGetter, ColumnVisitor
{
    protected static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

    protected final PageBuilder to;
    private final Type toType;

    public AbstractColumnGetter(PageBuilder to, Type toType)
    {
        this.to = to;
        this.toType = toType;
    }

    @Override
    public void getAndSet(ResultSet from, int fromIndex,
            Column toColumn) throws SQLException
    {
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
    public void jsonColumn(Column column)
    {
        to.setNull(column);
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

    @Override
    public JsonNode encodeToJson()
    {
        throw new DataException(String.format(ENGLISH,
                            "Column type '%s' set at incremental_columns option is not supported",
                            getToType()));
    }

    @Override
    public void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
        throws SQLException
    {
        throw new DataException(String.format(ENGLISH,
                            "Converting last_record value %s to column index %d is not supported",
                            fromValue.toString(), toIndex));
    }
}
