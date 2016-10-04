package org.embulk.input.oracle;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.embulk.input.AbstractJdbcInputPluginTest;
import org.embulk.input.OracleInputPlugin;
import org.embulk.spi.InputPlugin;
import org.junit.Test;

public class OracleInputPluginTest extends AbstractJdbcInputPluginTest
{
    @Override
    protected void prepare() throws SQLException {
        tester.addPlugin(InputPlugin.class, "oracle", OracleInputPlugin.class);

        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("Warning: you should put 'ojdbc7.jar' in 'embulk-input-oracle/driver' directory in order to test.");
            return;
        }

        Connection connection;
        try {
            connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/TESTDB", "TEST_USER", "test_pw");
        } catch (SQLException e) {
            System.err.println(e);
            System.err.println("Warning: you should prepare a schema on Oracle (database = 'TESTDB', user = 'TEST_USER', password = 'test_pw').");
            return;
        }

        try {
            try (Statement statement = connection.createStatement()) {
                String drop1 = "DROP TABLE TEST1";
                try {
                    statement.execute(drop1);
                } catch (SQLException e) {
                    System.out.println(e);
                }

                String create1 =
                        "CREATE TABLE TEST1 ("
                        + "C1  DECIMAL(12,2),"
                        + "C2  CHAR(8),"
                        + "C3  VARCHAR2(8),"
                        + "C4  NVARCHAR2(8),"
                        + "C5  DATE,"
                        + "C6  TIMESTAMP,"
                        + "C7  TIMESTAMP(3))";
                statement.execute(create1);

                String insert1 =
                        "INSERT INTO TEST1 VALUES("
                        + "NULL,"
                        + "NULL,"
                        + "NULL,"
                        + "NULL,"
                        + "NULL,"
                        + "NULL,"
                        + "NULL)";
                statement.executeUpdate(insert1);

                String insert2 =
                        "INSERT INTO TEST1 VALUES("
                        + "-1234567890.12,"
                        + "'ABCDEF',"
                        + "'XYZ',"
                        + "'ＡＢＣＤＥＦＧＨ',"
                        + "'2015-06-04',"
                        + "'2015-06-05 23:45:06',"
                        + "'2015-06-06 23:45:06.789')";
                statement.executeUpdate(insert2);
            }

        } finally {
            connection.close();
            enabled = true;
        }
    }

    @Test
    public void test() throws Exception
    {
        if (enabled) {
            test("/oracle/yml/input.yml");
            assertEquals(Arrays.asList(
                    "C1,C2,C3,C4,C5,C6,C7",
                    "-1.23456789012E9,ABCDEF  ,XYZ,ＡＢＣＤＥＦＧＨ,2015-06-04,2015-06-05 23:45:06,2015-06-06 23:45:06.789",
                    ",,,,,,"),
                    read("oracle-input000.00.csv"));
        }
    }

    @Test
    public void testLower() throws Exception
    {
        if (enabled) {
            test("/oracle/yml/input-lower.yml");
            assertEquals(Arrays.asList(
                    "C1,C2,C3,C4,C5,C6,C7",
                    "-1.23456789012E9,ABCDEF  ,XYZ,ＡＢＣＤＥＦＧＨ,2015-06-04,2015-06-05 23:45:06,2015-06-06 23:45:06.789",
                    ",,,,,,"),
                    read("oracle-input000.00.csv"));
        }
    }

    @Test
    public void testQuery() throws Exception
    {
        if (enabled) {
            test("/oracle/yml/input-query.yml");
            assertEquals(Arrays.asList(
                    "C1,C2,C3,C4,C5,C6,C7",
                    ",,,,,,",
                    "-1.23456789012E9,ABCDEF  ,XYZ,ＡＢＣＤＥＦＧＨ,2015-06-04,2015-06-05 23:45:06,2015-06-06 23:45:06.789"),
                    read("oracle-input000.00.csv"));
        }
    }

    @Test
    public void testQueryLower() throws Exception
    {
        if (enabled) {
            test("/oracle/yml/input-query-lower.yml");
            assertEquals(Arrays.asList(
                    "C1,C2,C3,C4,C5,C6,C7",
                    ",,,,,,",
                    "-1.23456789012E9,ABCDEF  ,XYZ,ＡＢＣＤＥＦＧＨ,2015-06-04,2015-06-05 23:45:06,2015-06-06 23:45:06.789"),
                    read("oracle-input000.00.csv"));
        }
    }

    @Test
    public void testColumnOptions() throws Exception
    {
        if (enabled) {
            test("/oracle/yml/input-column-options.yml");
            assertEquals(Arrays.asList(
                    "C1,C2,C3,C4,C5,C6,C7",
                    ",,,,,,",
                    "-1.23456789012E9,ABCDEF  ,XYZ,ＡＢＣＤＥＦＧＨ,2015/06/04,2015/06/05 23:45:06,2015/06/06 23:45:06.789"),
                    read("oracle-input000.00.csv"));
        }
    }

    @Test
    public void testColumnOptionsLower() throws Exception
    {
        if (enabled) {
            test("/oracle/yml/input-column-options-lower.yml");
            assertEquals(Arrays.asList(
                    "C1,C2,C3,C4,C5,C6,C7",
                    ",,,,,,",
                    "-1.23456789012E9,ABCDEF  ,XYZ,ＡＢＣＤＥＦＧＨ,2015/06/04,2015/06/05 23:45:06,2015/06/06 23:45:06.789"),
                    read("oracle-input000.00.csv"));
        }
    }

    private List<String> read(String path) throws IOException
    {
        FileSystem fs = FileSystems.getDefault();
        return Files.readAllLines(fs.getPath(path), Charset.forName("UTF8"));
    }

    @Override
    protected Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/TESTDB", "TEST_USER", "test_pw");
    }
}
