package org.embulk.input.sqlserver;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SQLServerPreviewQueryBuilder
{
    private static final String TOP_STATEMENT_TEMPLATE = "%s TOP 100 %s";

    private String query;

    public SQLServerPreviewQueryBuilder(final String query)
    {
        this.query = query;
    }

    public String build()
    {
        final List<String> queryTokens = Pattern.compile("\\s+")
            .splitAsStream(query)
            .collect(Collectors.toList());
        queryTokens.remove(0);

        return String.format(TOP_STATEMENT_TEMPLATE, "SELECT", queryTokens.stream().collect(Collectors.joining(" ")));
    }
}
