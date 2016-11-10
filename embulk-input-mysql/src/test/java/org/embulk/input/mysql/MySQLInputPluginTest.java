package org.embulk.input.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.embulk.input.AbstractJdbcInputPluginTest;
import org.embulk.input.MySQLInputPlugin;
import org.embulk.spi.InputPlugin;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static java.util.Locale.ENGLISH;

public class MySQLInputPluginTest extends AbstractJdbcInputPluginTest
{
    @Override
    protected void prepare() throws SQLException
    {
        tester.addPlugin(InputPlugin.class, "mysql", MySQLInputPlugin.class);

        try {
            connect();
        } catch (SQLException e) {
            System.err.println(e);
            System.err.println(String.format(ENGLISH, "Warning: prepare a schema on MySQL (server = %s, port = %d, database = %s, user = %s, password = %s).",
                    getHost(), getPort(), getDatabase(), getUser(), getPassword()));
            return;
        }

        enabled = true;

        String drop1 = "drop table if exists test1";
        executeSQL(drop1);

        String create1 =
                "create table test1 ("
                + "id  char(2),"
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
                + "c15 datetime(6),"
                + "primary key(id));";
        executeSQL(create1);

        String insert1 =
                "insert into test1 values("
                + "'10',"
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
        executeSQL(insert1);

        String insert2 =
                "insert into test1 values("
                + "'11',"
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
        executeSQL(insert2);

        String drop2 = "drop table if exists test2";
        executeSQL(drop2);

        String create2 = "create table test2 (c1 bigint unsigned);";
        executeSQL(create2);

        String insert3 = "insert into test2 values(18446744073709551615)";
        executeSQL(insert3);
    }

    @Test
    public void test() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input.yml");
            assertEquals(Arrays.asList(
                    "id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    "10,,,,,,,,,,,,,2015-06-04 20:45:06,,",
                    "11,99,9999,-99999999,-9999999999999999,1.2345000505447388,1.234567890123,-1234.0,1.2345678901234568E17,5678,xy,2015-06-03,2015-06-04 09:34:56,2015-06-04 20:45:06,06:04:02,2015-06-03 22:02:03"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testString() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-string.yml");
            assertEquals(Arrays.asList(
                    "id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    "10,,,,,,,,,,,,,2015-06-04 20:45:06,,",
                    "11,99,9999,-99999999,-9999999999999999,1.2345,1.234567890123,-1234,123456789012345678.12,5678,xy,2015-06-03,2015-06-04 09:34:56,2015-06-04 20:45:06,06:04:02,2015-06-03 22:02:03"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testBoolean() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-boolean.yml");
            assertEquals(Arrays.asList(
                    "id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    "10,,,,,,,,,,,,,2015-06-04 20:45:06,,",
                    "11,true,true,false,false,true,true,false,true,,,2015-06-03,2015-06-04 09:34:56,2015-06-04 20:45:06,06:04:02,2015-06-03 22:02:03"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testLong() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-long.yml");
            assertEquals(Arrays.asList(
                    "id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    "10,,,,,,,,,,,,,2015-06-04 20:45:06,,",
                    "11,99,9999,-99999999,-9999999999999999,1,1,-1234,123456789012345678,5678,,2015-06-03,2015-06-04 09:34:56,2015-06-04 20:45:06,06:04:02,2015-06-03 22:02:03"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testDouble() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-double.yml");
            assertEquals(Arrays.asList(
                    "id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    "10,,,,,,,,,,,,,2015-06-04 20:45:06,,",
                    "11,99.0,9999.0,-9.9999999E7,-1.0E16,1.2345000505447388,1.234567890123,-1234.0,1.2345678901234568E17,5678.0,,2015-06-03,2015-06-04 09:34:56,2015-06-04 20:45:06,06:04:02,2015-06-03 22:02:03"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testTimestamp1() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-timestamp1.yml");
            assertEquals(Arrays.asList(
                    "id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    "10,,,,,,,,,,,,,2015/06/04 20:45:06,,",
                    "11,99,9999,-99999999,-9999999999999999,1.2345000505447388,1.234567890123,-1234.0,1.2345678901234568E17,5678,xy,2015/06/03,2015/06/04 09:34:56,2015/06/04 20:45:06,06-04-02,2015/06/03 22:02:03.123456"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testTimestamp2() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-timestamp2.yml");
            assertEquals(Arrays.asList(
                    "id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    "10,,,,,,,,,,,,,2015/06/05 05:45:06,,",
                    "11,99,9999,-99999999,-9999999999999999,1.2345000505447388,1.234567890123,-1234.0,1.2345678901234568E17,5678,xy,2015/06/03,2015/06/04 09:34:56,2015/06/05 05:45:06,15-04-02,2015/06/03 22:02:03.123456"),
                    read("mysql-input000.00.csv"));
        }
    }

    @Test
    public void testTimestamp3() throws Exception
    {
        if (enabled) {
            test("/mysql/yml/input-timestamp3.yml");
            assertEquals(Arrays.asList(
                    "id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15",
                    "10,,,,,,,,,,,,,2015/06/05 05:45:06,,",
                    "11,99,9999,-99999999,-9999999999999999,1.2345000505447388,1.234567890123,-1234.0,1.2345678901234568E17,5678,xy,2015/06/04,2015/06/04 18:34:56,2015/06/05 05:45:06,09-04-02,2015/06/04 07:02:03.123456"),
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

    @Override
    protected Connection connect() throws SQLException
    {
        return DriverManager.getConnection(String.format(ENGLISH, "jdbc:mysql://%s:%d/%s", getHost(), getPort(), getDatabase()),
                getUser(), getPassword());
    }
}
