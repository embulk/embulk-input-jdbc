package org.embulk.input.cache;

import org.embulk.input.jdbc.JdbcInputConnection;

import java.sql.Connection;
import java.sql.SQLException;

public class CacheInputConnection
        extends JdbcInputConnection {

    public CacheInputConnection(Connection connection, String schemaName) throws SQLException {
        super(connection, schemaName);
    }

    @Override
    protected void setSearchPath(String schema) throws SQLException {
        //NOP
    }

    @Override
    protected String buildTableName(String tableName)
    {
        StringBuilder sb = new StringBuilder();
        if (schemaName != null) {
            sb.append(quoteIdentifierString(schemaName)).append(".");
        }
        sb.append(quoteIdentifierString(tableName));
        return sb.toString();
    }

}
