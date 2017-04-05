package org.embulk.input.redshift;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTests;

import java.io.IOException;

import static java.util.Locale.ENGLISH;

public class RedshiftTests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_INPUT_REDSHIFT_TEST_CONFIG");
    }

    public static void execute(String sql)
    {
        ConfigSource config = baseConfig();
        ProcessBuilder pb = new ProcessBuilder("psql", "-w", "--set", "ON_ERROR_STOP=1", "-c", sql);
        pb.environment().put("PGUSER", config.get(String.class, "user"));
        pb.environment().put("PGPASSWORD", config.get(String.class, "password"));
        pb.environment().put("PGDATABASE", config.get(String.class, "database"));
        pb.environment().put("PGHOST", config.get(String.class, "host", "localhost"));
        pb.environment().put("PGPORT", config.get(String.class, "port", "5439"));
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
}
