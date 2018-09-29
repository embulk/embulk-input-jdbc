package org.embulk.input.postgresql;

import org.embulk.test.EmbulkTests;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import jnr.ffi.Platform;
import jnr.ffi.Platform.OS;
import org.embulk.config.ConfigSource;
import static java.util.Locale.ENGLISH;

public class PostgreSQLTests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_INPUT_POSTGRESQL_TEST_CONFIG");
    }

    public static void execute(String sql)
    {
        ConfigSource config = baseConfig();
        ProcessBuilder pb = new ProcessBuilder("psql", "-w", "--set", "ON_ERROR_STOP=1", "-c", convert(sql));
        pb.environment().put("PGUSER", config.get(String.class, "user"));
        pb.environment().put("PGPASSWORD", config.get(String.class, "password"));
        pb.environment().put("PGDATABASE", config.get(String.class, "database"));
        pb.environment().put("PGPORT", config.get(String.class, "port", "5432"));
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
