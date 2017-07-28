package org.embulk.input.db2;

import static java.util.Locale.ENGLISH;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTests;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;

public class DB2Tests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_INPUT_DB2_TEST_CONFIG");
    }

    public static void execute(String sqlName) throws Exception
    {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("You should put 'db2jcc4.jar' in 'embulk-input-db2/test_jdbc_driver' directory in order to test.");
        }

        // DB2Tests.excute takes a resource name of SQL file, doesn't take a SQL sentence as other XXXTests do.
        // Because TestingEmbulk.createTempFile might create a file whose name contains ' ' and DB2 clpplus cannot read such a file.
        // But if root directory name of embulk-input-db2 contains ' ', tests will fail for the same reason.
        URL sqlRrl = DB2Tests.class.getResource("/" + sqlName);

        ConfigSource config = baseConfig();
        String host = config.get(String.class, "host");
        String port = config.get(String.class, "port", "50000");
        String user = config.get(String.class, "user");
        String password = config.get(String.class, "password");
        String database = config.get(String.class, "database");

        boolean isWindows = File.separatorChar == '\\';
        ProcessBuilder pb = new ProcessBuilder(
                "clpplus." + (isWindows ? "bat" : "sh"),
                user + "/" + password + "@" + host + ":" + port + "/" + database,
                "@" + new File(sqlRrl.toURI()).getAbsolutePath());
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
