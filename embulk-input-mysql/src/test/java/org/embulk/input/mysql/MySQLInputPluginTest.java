package org.embulk.input.mysql;

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
import org.embulk.input.MySQLInputPlugin;
import org.embulk.spi.InputPlugin;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MySQLInputPluginTest extends AbstractJdbcInputPluginTest
{
    @Override
    protected void prepare() throws SQLException {
        tester.addPlugin(InputPlugin.class, "mysql", MySQLInputPlugin.class);

        Connection connection;
        try {
            connection = DriverManager.getConnection("jdbc:mysql://localhost/TESTDB", "TEST_USER", "test_pw");
        } catch (SQLException e) {
            System.err.println(e);
            System.err.println("Warning: prepare a schema on MySQL (database = 'TESTDB', user = 'TEST_USER', password = 'test_pw').");
            return;
        }

        try {
            try (Statement statement = connection.createStatement()) {
                String drop1 = "drop table if exists test1";
                statement.execute(drop1);

                String create1 =
                        "create table test1 ("
                        + "c1  tinyint,"
                        + "c2  smallint,"
                        + "c3  int,"
                        + "c4  bigint,"
                        + "c5  float,"
                        + "c6  double,"
                        + "c7  decimal(4,0),"
                        + "c8  decimal(20,2),"
                        + "c9  char(4),"
                        + "c10 varchar(4),"
                        + "c11 date,"
                        + "c12 datetime,"
                        + "c13 timestamp,"
                        + "c14 time,"
                        + "c15 datetime(6));";
                statement.execute(create1);

                String insert1 =
                        "insert into test1 values("
                        + "null,"
                        + "null,"
                        + "null,"
                        + "null,"
                        + "null,"
                        + "null,"
                        + "null,"
                        + "null,"
                        + "null,"
                        + "null,"
                        + "null,"
                        + "null,"
                        + "'2015-06-04 23:45:06',"
                        + "null,"
                        + "null);";
                statement.executeUpdate(insert1);

                String insert2 =
                        "insert into test1 values("
                        + "99,"
                        + "9999,"
                        + "-99999999,"
                        + "-9999999999999999,"
                        + "1.2345,"
                        + "1.234567890123,"
                        + "-1234,"
                        + "123456789012345678.12,"
                        + "'5678',"
                        + "'xy',"
                        + "'2015-06-04',"
                        + "'2015-06-04 12:34:56',"
                        + "'2015-06-04 23:45:06',"
                        + "'08:04:02',"
                        + "'2015-06-04 01:02:03.123456');";
                statement.executeUpdate(insert2);

                String drop2 = "drop table if exists test2";
                statement.execute(drop2);

                String create2 = "create table test2 (c1 bigint unsigned);";
                statement.execute(create2);

                String insert3 = "insert into test2 values(18446744073709551615)";
                statement.executeUpdate(insert3);
            }

        } finally {
            connection.close();
            enabled = true;
        }
    }

    /*
    @AfterClass
    public static void dispose()
    {
        tester.destroy();
    }
    */

    @Test
    public void test() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input.yml");
            assertEquals(Arrays.asList(
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    ",,,,,,,,,,,,2015-06-04 14:45:06,,",
                    "99,9999,-99999999,-9999999999999999,1.2345000505447388,1.234567890123,-1234.0,1.2345678901234568E17,5678,xy,2015-06-03,2015-06-04 03:34:56,2015-06-04 14:45:06,23:04:02,2015-06-03 16:02:03"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testString() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-string.yml");
            assertEquals(Arrays.asList(
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    ",,,,,,,,,,,,2015-06-04 14:45:06,,",
                    "99,9999,-99999999,-9999999999999999,1.2345,1.234567890123,-1234,123456789012345678.12,5678,xy,2015-06-03,2015-06-04 03:34:56,2015-06-04 14:45:06,23:04:02,2015-06-03 16:02:03"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testBoolean() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-boolean.yml");
            assertEquals(Arrays.asList(
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    ",,,,,,,,,,,,2015-06-04 14:45:06,,",
                    "true,true,false,false,true,true,false,true,,,2015-06-03,2015-06-04 03:34:56,2015-06-04 14:45:06,23:04:02,2015-06-03 16:02:03"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testLong() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-long.yml");
            assertEquals(Arrays.asList(
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    ",,,,,,,,,,,,2015-06-04 14:45:06,,",
                    "99,9999,-99999999,-9999999999999999,1,1,-1234,123456789012345678,5678,,2015-06-03,2015-06-04 03:34:56,2015-06-04 14:45:06,23:04:02,2015-06-03 16:02:03"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testDouble() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-double.yml");
            assertEquals(Arrays.asList(
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    ",,,,,,,,,,,,2015-06-04 14:45:06,,",
                    "99.0,9999.0,-9.9999999E7,-1.0E16,1.2345000505447388,1.234567890123,-1234.0,1.2345678901234568E17,5678.0,,2015-06-03,2015-06-04 03:34:56,2015-06-04 14:45:06,23:04:02,2015-06-03 16:02:03"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testTimestamp1() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-timestamp1.yml");
            assertEquals(Arrays.asList(
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    ",,,,,,,,,,,,2015/06/04 14:45:06,,",
                    "99,9999,-99999999,-9999999999999999,1.2345000505447388,1.234567890123,-1234.0,1.2345678901234568E17,5678,xy,2015/06/03,2015/06/04 03:34:56,2015/06/04 14:45:06,23-04-02,2015/06/03 16:02:03.123456"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testTimestamp2() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-timestamp2.yml");
            assertEquals(Arrays.asList(
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    ",,,,,,,,,,,,2015/06/04 23:45:06,,",
                    "99,9999,-99999999,-9999999999999999,1.2345000505447388,1.234567890123,-1234.0,1.2345678901234568E17,5678,xy,2015/06/03,2015/06/04 03:34:56,2015/06/04 23:45:06,08-04-02,2015/06/03 16:02:03.123456"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testTimestamp3() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-timestamp3.yml");
            assertEquals(Arrays.asList(
                    "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    ",,,,,,,,,,,,2015/06/04 23:45:06,,",
                    "99,9999,-99999999,-9999999999999999,1.2345000505447388,1.234567890123,-1234.0,1.2345678901234568E17,5678,xy,2015/06/04,2015/06/04 12:34:56,2015/06/04 23:45:06,02-04-02,2015/06/04 01:02:03.123456"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testValueTypeString() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-valuetype-string.yml");
            assertEquals(Arrays.asList(
                    "c1",
                    "18446744073709551615"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testValueTypeDecimal() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-valuetype-decimal.yml");
            assertEquals(Arrays.asList(
                    "c1",
                    "1.8446744073709552E19"),
                    read("mysql-input000.00.csv"));
        }
    }

    private List<String> read(String path) throws IOException
    {
        FileSystem fs = FileSystems.getDefault();
        return Files.readAllLines(fs.getPath(path), Charset.defaultCharset());
    }

    /*
    private String convertPath(String name) throws URISyntaxException
    {
        if (getClass().getResource(name) == null) {
            return name;
        }
        return new File(getClass().getResource(name).toURI()).getAbsolutePath();
    }
    */

    @Override
    protected Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost/TESTDB", "TEST_USER", "test_pw");
    }
}
