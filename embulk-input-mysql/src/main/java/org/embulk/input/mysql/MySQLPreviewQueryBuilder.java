package org.embulk.input.mysql;

public class MySQLPreviewQueryBuilder
{
    private static final String LIMIT_STATEMENT_TEMPLATE = "%s LIMIT 100";

    private String query;

    public MySQLPreviewQueryBuilder(final String query)
    {
        this.query = query;
    }

    public String build()
    {
        return String.format(LIMIT_STATEMENT_TEMPLATE, this.query);
    }
}
