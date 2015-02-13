package org.embulk.input.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class JdbcColumn
{
    private String name;
    private String typeName;
    private int sqlType;

    @JsonCreator
    public JdbcColumn(
            @JsonProperty("name") String name,
            @JsonProperty("typeName") String typeName,
            @JsonProperty("sqlType") int sqlType)
    {
        this.name = name;
        this.typeName = typeName;
        this.sqlType = sqlType;
    }

    @JsonProperty("name")
    public String getName()
    {
        return name;
    }

    @JsonProperty("typeName")
    public String getTypeName()
    {
        return typeName;
    }

    @JsonProperty("sqlType")
    public int getSqlType()
    {
        return sqlType;
    }
}
