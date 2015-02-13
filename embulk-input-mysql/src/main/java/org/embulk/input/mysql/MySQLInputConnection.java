package org.embulk.input.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.embulk.input.jdbc.JdbcInputConnection;

public class MySQLInputConnection
        extends JdbcInputConnection
{
    public MySQLInputConnection(Connection connection)
            throws SQLException
    {
        super(connection, null);
    }
}
