package org.embulk.input.sqlserver;

import java.sql.Connection;
import java.sql.SQLException;
import org.embulk.input.jdbc.JdbcInputConnection;

public class SQLServerInputConnection extends JdbcInputConnection {

    private String tableHint;

    public SQLServerInputConnection(Connection connection, String schemaName) throws SQLException
    {
        super(connection, schemaName);
    }

    public SQLServerInputConnection(Connection connection, String schemaName, String tableHint) throws SQLException
    {
        super(connection, schemaName);
        this.tableHint = tableHint;
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
        if (tableHint != null) {
            sb.append(" with (" + tableHint + ")");
        }
        return sb.toString();
    }

}
