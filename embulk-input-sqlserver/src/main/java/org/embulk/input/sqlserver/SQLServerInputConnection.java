package org.embulk.input.sqlserver;

import java.sql.Connection;
import java.sql.SQLException;
import org.embulk.input.jdbc.JdbcInputConnection;

public class SQLServerInputConnection extends JdbcInputConnection {

    public SQLServerInputConnection(Connection connection, String schemaName) throws SQLException
    {
        super(connection, schemaName);
    }

    @Override
    protected void setSearchPath(String schema) throws SQLException
    {
        // NOP
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
