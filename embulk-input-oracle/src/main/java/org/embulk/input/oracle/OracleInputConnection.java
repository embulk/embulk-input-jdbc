package org.embulk.input.oracle;

import java.sql.Connection;
import java.sql.SQLException;

import org.embulk.input.jdbc.JdbcInputConnection;

public class OracleInputConnection extends JdbcInputConnection {

    public OracleInputConnection(Connection connection, String schemaName) throws SQLException
    {
        super(connection, schemaName);
    }

    @Override
    protected void setSearchPath(String schema) throws SQLException
    {
        // NOP
    }

}
