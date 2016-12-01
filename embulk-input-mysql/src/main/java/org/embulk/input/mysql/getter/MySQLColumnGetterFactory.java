package org.embulk.input.mysql.getter;

import com.google.common.base.Optional;
import org.embulk.config.ConfigException;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin.PluginTask;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.mysql.MySQLInputConnection;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkNotNull;

public class MySQLColumnGetterFactory
        extends ColumnGetterFactory
{
    public MySQLColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone)
    {
        super(to, defaultTimeZone);
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcInputConnection con, PluginTask task, JdbcColumn column, JdbcColumnOption option)
    {
        ColumnGetter getter = super.newColumnGetter(con, task, column, option);

        switch (column.getTypeName()) {
        case "DATETIME":
        case "TIMESTAMP":
            int index = task.getQuerySchema().findColumn(column.getName()).get();
            if (!task.getIncremental() || !task.getIncrementalColumnIndexes().contains(index)) {
                return getter;
            }

            // incremental loading
            MySQLInputConnection mysqlInputConnection = (MySQLInputConnection) con;
            // Users cannot use DATETIME or TIMESTAMP typed columns as incremental_columns: if 'useLegacyDatetimeCode=true'.
            // That might be acceptable since mysql-connector-java v6.x will turn off, by default.
            if (mysqlInputConnection.getUseLegacyDatetimeCode()) {
                throw new ConfigException("Must use 'useLegacyDatetimeCode=false' if 'DATETIME' or 'TIMESTAMP' typed columns are used as incremental_columns:");
            }

            TimeZone timeZone = mysqlInputConnection.getServerTimezoneTZ();
            // Joda-Time's timezone mapping is probably not compatible with java.util.TimeZone if null is returned.
            DateTimeZone sessionTimeZone = checkNotNull(DateTimeZone.forTimeZone(timeZone));
            if (column.getTypeName().equals("DATETIME")) {
                return new MySQLDateTimeTimestampIncrementalHandler(sessionTimeZone, getter);
            }
            else { // TIMESTAMP
                return new MySQLTimestampTimestampIncrementalHandler(sessionTimeZone, getter);
            }
        default:
            return getter;
        }
    }
}
