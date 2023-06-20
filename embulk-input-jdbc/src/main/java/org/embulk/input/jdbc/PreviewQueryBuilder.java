package org.embulk.input.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;

public class PreviewQueryBuilder
{
    private static final String MSSQL_PRODUCT_NAME = "Microsoft SQL Server";
    private static final String LIMIT_STATEMENT_TEMPLATE = "%s LIMIT 50";
    private static final String TOP_STATEMENT_TEMPLATE = "%s TOP 50 %s";
    private static final Pattern SELECT_PATTERN = Pattern.compile("select", Pattern.CASE_INSENSITIVE);

    private String query;
    private Connection connection;

    public PreviewQueryBuilder(final String query, final Connection connection)
    {
        this.query = query;
        this.connection = connection;
    }

    public String build() throws SQLException
    {
        return isMSSQL()
            ? buildPreviewSQLForMSSQL()
            : String.format(LIMIT_STATEMENT_TEMPLATE, this.query);
    }

    private String buildPreviewSQLForMSSQL()
    {
        final String[] statements = SELECT_PATTERN.split(query);
        return String.format(TOP_STATEMENT_TEMPLATE, "SELECT", statements[1]);
    }

    protected boolean isMSSQL() throws SQLException
    {
        final String productName = connection.getMetaData().getDatabaseProductName();
        return MSSQL_PRODUCT_NAME.equals(productName);
    }
}
