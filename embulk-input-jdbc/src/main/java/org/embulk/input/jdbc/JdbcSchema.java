package org.embulk.input.jdbc;

import java.util.List;
import com.google.common.base.Optional;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class JdbcSchema
{
    private List<JdbcColumn> columns;

    @JsonCreator
    public JdbcSchema(List<JdbcColumn> columns)
    {
        this.columns = columns;
    }

    @JsonValue
    public List<JdbcColumn> getColumns()
    {
        return columns;
    }

    public int getCount()
    {
        return columns.size();
    }

    public JdbcColumn getColumn(int i)
    {
        return columns.get(i);
    }

    public String getColumnName(int i)
    {
        return columns.get(i).getName();
    }

    public Optional<Integer> findColumn(String caseInsensitiveName)
    {
        // find by case sensitive first
        for (int i = 0; i < columns.size(); i++) {
            if (getColumn(i).getName().equals(caseInsensitiveName)) {
                return Optional.of(i);
            }
        }
        // find by case insensitive
        for (int i = 0; i < columns.size(); i++) {
            if (getColumn(i).getName().equalsIgnoreCase(caseInsensitiveName)) {
                return Optional.of(i);
            }
        }
        return Optional.absent();
    }
}
