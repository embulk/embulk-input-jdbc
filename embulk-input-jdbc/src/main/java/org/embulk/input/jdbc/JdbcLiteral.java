package org.embulk.input.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public class JdbcLiteral
{
    private final int columnIndex;
    private final JsonNode value;

    @JsonCreator
    public JdbcLiteral(
            @JsonProperty("columnIndex") int columnIndex,
            @JsonProperty("value") JsonNode value)
    {
        this.columnIndex = columnIndex;
        this.value = value;
    }

    @JsonProperty("columnIndex")
    public int getColumnIndex()
    {
        return columnIndex;
    }

    @JsonProperty("value")
    public JsonNode getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return value.toString();
    }
}
