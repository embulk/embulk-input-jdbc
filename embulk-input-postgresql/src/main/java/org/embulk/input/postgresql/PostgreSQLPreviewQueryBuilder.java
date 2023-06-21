package org.embulk.input.postgresql;

public class PostgreSQLPreviewQueryBuilder
{
    private static final String LIMIT_STATEMENT_TEMPLATE = "%s LIMIT 100";

    private String query;

    public PostgreSQLPreviewQueryBuilder(final String query)
    {
        this.query = query;
    }

    public String build()
    {
        return String.format(LIMIT_STATEMENT_TEMPLATE, this.query);
    }
}
