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

}
