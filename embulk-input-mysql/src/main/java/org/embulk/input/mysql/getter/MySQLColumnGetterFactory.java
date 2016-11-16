package org.embulk.input.mysql.getter;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.ResultSetImpl;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

public class MySQLColumnGetterFactory
        extends ColumnGetterFactory
{
    public MySQLColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone)
    {
        super(to, defaultTimeZone);
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcColumn column, JdbcColumnOption option)
    {
        ColumnGetter getter = super.newColumnGetter(column, option);

        // incremental loading
        switch (column.getTypeName()) {
        case "DATETIME":
            return new MySQLDateTimeIncrementalHandler(getter);
        case "TIMESTAMP":
            return new MySQLTimestampIncrementalHandler(getter);
        default:
            return getter;
        }
    }

    static DateTimeZone getSessionTimeZone(ResultSet from)
    {
        Field f = null;
        try {
            // Need to check if the processing works fine or not to upgrade mysql-connector-java version.
            f = ResultSetImpl.class.getDeclaredField("serverTimeZoneTz");
            f.setAccessible(true);
            TimeZone timeZone = (TimeZone) f.get(from);

            // Joda-Time's timezone mapping is probably not compatible with java.util.TimeZone if null is returned.
            return Preconditions.checkNotNull(DateTimeZone.forTimeZone(timeZone));
        }
        catch (IllegalAccessException | NoSuchFieldException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if (f != null) {
                f.setAccessible(false);
            }
        }
    }

    static DateTimeZone getSessionTimeZone(java.sql.PreparedStatement from)
            throws SQLException
    {
        TimeZone timeZone = ((ConnectionImpl) from.getConnection()).getServerTimezoneTZ();
        // Joda-Time's timezone mapping is probably not compatible with java.util.TimeZone if null is returned.
        return Preconditions.checkNotNull(DateTimeZone.forTimeZone(timeZone));
    }
}
