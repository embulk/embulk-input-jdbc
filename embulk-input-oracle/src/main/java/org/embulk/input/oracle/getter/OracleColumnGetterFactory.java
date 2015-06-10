package org.embulk.input.oracle.getter;

import java.sql.Types;

import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.jdbc.getter.ColumnGetters.HighPrecisionTimestampColumnGetter;

public class OracleColumnGetterFactory extends ColumnGetterFactory
{
    @Override
    public ColumnGetter newColumnGetter(JdbcColumn column)
    {
        if (column.getSqlType() == Types.TIMESTAMP && column.getScale() > 0) {
            return new HighPrecisionTimestampColumnGetter(column.getScale());
        } else {
            return super.newColumnGetter(column);
        }
    }
}
