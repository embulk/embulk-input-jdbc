package org.embulk.input.clickhouse;

import org.embulk.input.clickhouse.getter.ArrayColumnGetter;
import org.embulk.input.clickhouse.getter.BigIntegerColumnGetter;
import org.embulk.input.clickhouse.getter.TupleColumnGetter;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

public class ClickHouseColumnGetterFactory extends ColumnGetterFactory {

    public ClickHouseColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone) {
        super(to, defaultTimeZone);
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcInputConnection con, AbstractJdbcInputPlugin.PluginTask task, JdbcColumn column, JdbcColumnOption option)
    {
        if ( column.getTypeName().toLowerCase().equals("uint64")) {
            return new BigIntegerColumnGetter(to, getToType(option), column.getTypeName());
        }

        if (column.getSqlType() == java.sql.Types.ARRAY) {
            return new ArrayColumnGetter(to, getToType(option));
        }

        if ( column.getTypeName().startsWith("Tuple")){
            return new TupleColumnGetter(to, getToType(option));
        }

        return super.newColumnGetter(con, task, column, option);
    }
}
