package org.embulk.input.db2;

import java.sql.Connection;
import java.sql.SQLException;

import org.embulk.input.jdbc.JdbcInputConnection;

public class DB2InputConnection extends JdbcInputConnection {

    public DB2InputConnection(Connection connection, String schemaName) throws SQLException
    {
        super(connection, schemaName);
    }

    @Override
    protected void setSearchPath(String schema) throws SQLException
    {
        connection.setSchema(schema);
    }

    @Override
    public void close() throws SQLException
    {
        // DB2 JDBC Driver requires explicit commit/rollback before closing connection.
        connection.rollback();

        super.close();
    }

}
