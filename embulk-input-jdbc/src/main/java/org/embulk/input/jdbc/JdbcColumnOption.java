package org.embulk.input.jdbc;

import java.time.ZoneId;
import java.util.Optional;
import org.embulk.spi.type.Type;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

public interface JdbcColumnOption
        extends Task
{
    @Config("value_type")
    @ConfigDefault("\"coalesce\"")
    public String getValueType();

    @Config("type")
    @ConfigDefault("null")
    public Optional<Type> getType();

    @Config("timestamp_format")
    @ConfigDefault("null")
    public Optional<String> getTimestampFormat();

    @Config("timezone")
    @ConfigDefault("null")
    public Optional<ZoneId> getTimeZone();
}
