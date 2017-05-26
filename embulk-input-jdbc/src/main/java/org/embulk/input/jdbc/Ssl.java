package org.embulk.input.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import org.embulk.config.ConfigException;

public enum Ssl
{
    ENABLE,
    DISABLE,
    VERIFY;

    @JsonValue
    @Override
    public String toString()
    {
        return this.name().toLowerCase();
    }

    @JsonCreator
    public static Ssl fromString(String value)
    {
        switch(value) {
        case "enable":
        case "true":
            return ENABLE;
        case "disable":
        case "false":
            return DISABLE;
        case "verify":
            return VERIFY;
        default:
            throw new ConfigException(String.format("Unknown SSL value '%s'. Supported values are enable, true, disable, false or verify.", value));
        }
    }
}
