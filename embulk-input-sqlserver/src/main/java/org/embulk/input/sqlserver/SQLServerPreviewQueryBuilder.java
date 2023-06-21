package org.embulk.input.sqlserver;

import java.util.regex.Pattern;

public class SQLServerPreviewQueryBuilder
{
    private static final String TOP_STATEMENT_TEMPLATE = "%s TOP 100 %s";
    private static final Pattern SELECT_PATTERN = Pattern.compile("select", Pattern.CASE_INSENSITIVE);

    private String query;

    public SQLServerPreviewQueryBuilder(final String query)
    {
        this.query = query;
    }

    public String build()
    {
        final String[] statements = SELECT_PATTERN.split(query);
        return String.format(TOP_STATEMENT_TEMPLATE, "SELECT", statements[1]);
    }
}
