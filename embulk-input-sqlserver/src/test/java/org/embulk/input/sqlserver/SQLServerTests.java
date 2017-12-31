package org.embulk.input.sqlserver;

import static java.util.Locale.ENGLISH;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;

public class SQLServerTests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_INPUT_SQLSERVER_TEST_CONFIG");
    }

    public static void execute(String sql, String... options)
    {
        ConfigSource config = baseConfig();

        ImmutableList.Builder<String> args = ImmutableList.builder();
        args.add("sqlcmd")
                .add("-U")
                .add(config.get(String.class, "user"))
                .add("-P")
                .add(config.get(String.class, "password"))
                .add("-H")
                .add(config.get(String.class, "host"))
                .add("-d")
                .add(config.get(String.class, "database"))
                .add("-Q")
                .add(sql);
        for (String option : options) {
            args.add(option);
        }

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

    public static String selectRecords(TestingEmbulk embulk, String tableName) throws IOException
    {
        Path temp = embulk.createTempFile("txt");
        Files.delete(temp);

        // should not use UTF8 because of BOM
        execute("SET NOCOUNT ON; SELECT * FROM " + tableName, "-h", "-1", "-s", ",", "-W", "-f", "932", "-o", temp.toString());

        List<String> lines = Files.readAllLines(temp, Charset.forName("MS932"));
        Collections.sort(lines);
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line);
            sb.append("\n");
        }
        return sb.toString();
    }
}
