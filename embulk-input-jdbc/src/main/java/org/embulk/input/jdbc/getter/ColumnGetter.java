package org.embulk.input.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;

public interface ColumnGetter
{
    public void getAndSet(ResultSet from, int fromIndex,
            PageBuilder to, Column toColumn) throws SQLException;

    public Type getToType();
}
