package org.embulk.input.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.embulk.input.jdbc.JdbcInputConnection;

public class OracleInputConnection extends JdbcInputConnection {

    public OracleInputConnection(Connection connection, String schemaName) throws SQLException
    {
        super(connection, schemaName == null ? getSchema(connection) : schemaName);
    }

    @Override
    protected void setSearchPath(String schema) throws SQLException
    {
        connection.setSchema(schema);
    }

    private static String getSchema(Connection connection) throws SQLException
    {
        // Because old Oracle JDBC drivers don't support Connection#getSchema method.
        String sql = "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
                throw new SQLException(String.format("Cannot get schema becase \"%s\" didn't return any value.", sql));
            }
        }
    }

}
