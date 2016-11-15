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

public abstract class AbstractIncrementalHandler implements ColumnGetter
{
    protected static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

    protected ColumnGetter next;

    public AbstractIncrementalHandler(ColumnGetter next)
    {
        this.next = next;
    }

    @Override
    public void getAndSet(ResultSet from, int fromIndex,
            Column toColumn) throws SQLException
    {
        next.getAndSet(from, fromIndex, toColumn);
    }

    @Override
    public Type getToType()
    {
        return next.getToType();
    }

    @Override
    public abstract JsonNode encodeToJson();

    @Override
    public abstract void decodeFromJsonTo(PreparedStatement toStatement, int toIndex, JsonNode fromValue)
        throws SQLException;
}
