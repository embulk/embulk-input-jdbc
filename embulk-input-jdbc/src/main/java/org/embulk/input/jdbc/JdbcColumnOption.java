package org.embulk.input.jdbc;

import java.util.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.Task;
import org.embulk.spi.time.TimestampFormat;
import org.embulk.spi.type.Type;

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
    public Optional<TimestampFormat> getTimestampFormat();

    @Config("timezone")
    @ConfigDefault("null")
    public Optional<String> getTimeZone();
}
