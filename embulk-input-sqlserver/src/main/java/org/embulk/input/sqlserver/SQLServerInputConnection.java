package org.embulk.input.sqlserver;

import java.sql.Connection;
import java.sql.SQLException;

import org.embulk.input.jdbc.JdbcInputConnection;

import com.google.common.base.Optional;

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
    public String buildSelectQuery(String tableName,
            Optional<String> selectColumnList, Optional<String> whereCondition,
            Optional<String> orderByColumn)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        sb.append(selectColumnList.or("*"));
        sb.append(" FROM ");
        if (schemaName != null) {
            sb.append(quoteIdentifierString(schemaName)).append(".");
        }
        sb.append(quoteIdentifierString(tableName));
        if (whereCondition.isPresent()) {
            sb.append(" WHERE ").append(whereCondition.get());
        }
        if (orderByColumn.isPresent()) {
            sb.append("ORDER BY ").append(quoteIdentifierString(orderByColumn.get())).append(" ASC");
        }

        return sb.toString();
    }
}
