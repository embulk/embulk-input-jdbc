package org.embulk.input.mysql;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;

import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTests;

import java.io.IOException;

import jnr.ffi.Platform;
import jnr.ffi.Platform.OS;
import static java.util.Locale.ENGLISH;

public class MySQLTests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_INPUT_MYSQL_TEST_CONFIG");
    }

    public static void execute(String sql)
    {
        ConfigSource config = baseConfig();

        ImmutableList.Builder<String> args = ImmutableList.builder();
        args.add("mysql")
                .add("-u")
                .add(config.get(String.class, "user"));
        if (!config.get(String.class, "password").isEmpty()) {
            args.add("-p" + config.get(String.class, "password"));
        }
        args
                .add("-h")
                .add(config.get(String.class, "host"))
                .add("-P")
                .add(config.get(String.class, "port", "3306"))
                .add(config.get(String.class, "database"))
                .add("-e")
                .add(convert(sql));

        ProcessBuilder pb = new ProcessBuilder(args.build());
        pb.redirectErrorStream(true);
        int code;
        try {
            Process process = pb.start();
            ByteStreams.copy(process.getInputStream(), System.out);
            code = process.waitFor();
        } catch (IOException | InterruptedException ex) {
            throw Throwables.propagate(ex);
        }
        if (code != 0) {
            throw new RuntimeException(String.format(ENGLISH,
                    "Command finished with non-zero exit code. Exit code is %d.", code));
        }
    }

    private static String convert(String sql)
    {
        if (Platform.getNativePlatform().getOS().equals(OS.WINDOWS)) {
            // '"' should be '\"' and '\' should be '\\' in Windows
            return sql.replace("\\\\", "\\").replace("\\", "\\\\").replace("\"", "\\\"");
        }
        return sql;
    }
}
