package org.embulk.input.jdbc.getter;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import com.fasterxml.jackson.databind.JsonNode;

public class TimestampIncrementalHadlerTestBase
{
    protected PageBuilder createPageBuilder()
    {
        BufferAllocator allocator = new BufferAllocator() {
            @Override
            public Buffer allocate(int minimumCapacity) {
                return Buffer.allocate(minimumCapacity);
            }

            @Override
            public Buffer allocate() {
                return allocate(16);
            }
        };
        return new PageBuilder(allocator, new Schema(Arrays.asList(new Column(0, null, Types.TIMESTAMP))), null);
    }

    protected Timestamp createTimestamp(String datetime, int nanos) throws ParseException
    {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = dateFormat.parse(datetime);
        Timestamp timestamp = new Timestamp(date.getTime());
        timestamp.setNanos(nanos);
        return timestamp;
    }

    protected void setTimestamp(ColumnGetter getter, final Timestamp value) throws SQLException
    {
        ResultSet resultSetProxy = (ResultSet)Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{ResultSet.class}, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args)
                    throws Throwable {
                if (method.getName().equals("getTimestamp")) {
                    return value;
                }
                if (method.getName().equals("wasNull")) {
                    return false;
                }
                throw new UnsupportedOperationException(method.getName());
            }
        });
        getter.getAndSet(resultSetProxy, 0, new Column(0, null, Types.TIMESTAMP));
    }

    protected Timestamp decodeTimestamp(ColumnGetter getter, JsonNode value) throws SQLException
    {
        final Timestamp[] result = new Timestamp[]{null};
        PreparedStatement statementProxy = (PreparedStatement)Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{PreparedStatement.class}, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args)
                    throws Throwable {
                if (method.getName().equals("setTimestamp")) {
                    result[0] = (Timestamp)args[1];
                    return null;
                }
                throw new UnsupportedOperationException(method.getName());
            }
        });
        getter.decodeFromJsonTo(statementProxy, 0, value);
        return result[0];
    }

}
