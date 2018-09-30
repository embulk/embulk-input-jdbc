package org.embulk.input.clickhouse;

import org.embulk.input.jdbc.JdbcInputConnection;

import java.sql.Connection;
import java.sql.SQLException;

public class ClickHouseInputConnection extends JdbcInputConnection {
    public ClickHouseInputConnection(Connection connection, String schemaName)
            throws SQLException
    {
        super(connection, schemaName);
    }
}
