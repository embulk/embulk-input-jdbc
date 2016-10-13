package org.embulk.input.db2;

import static java.util.Locale.ENGLISH;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.embulk.input.AbstractJdbcInputPluginTest;
import org.embulk.input.DB2InputPlugin;
import org.embulk.spi.InputPlugin;
import org.junit.Test;

public class DB2InputPluginTest extends AbstractJdbcInputPluginTest
{
    @Override
    protected void prepare() throws SQLException
    {
        tester.addPlugin(InputPlugin.class, "db2", DB2InputPlugin.class);

        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Warning: you should put 'db2jcc4.jar' in 'embulk-input-db2/driver' directory in order to test.");
            return;
        }

        try {
            connect();
        } catch (SQLException e) {
            System.err.println(e);
            System.err.println(String.format(ENGLISH, "Warning: prepare a schema on DB2 (server = %s, port = %d, database = %s, user = %s, password = %s).",
                    getHost(), getPort(), getDatabase(), getUser(), getPassword()));
            return;
        }

        enabled = true;

        String dropString = "DROP TABLE TEST_STRING";
        executeSQL(dropString, true);

        String createString =
                "CREATE TABLE TEST_STRING ("
                + "ID               CHAR(2) NOT NULL,"
                + "CHAR_ITEM        CHAR(4),"
                + "VARCHAR_ITEM     VARCHAR(8),"
                + "CLOB_ITEM        CLOB,"
                + "GRAPHIC_ITEM     GRAPHIC(4),"
                + "VARGRAPHIC_ITEM  VARGRAPHIC(8),"
                + "NCHAR_ITEM       NCHAR(4),"
                + "NVARCHAR_ITEM    NVARCHAR(8),"
                + "NCLOB_ITEM       NCLOB,"
                + "PRIMARY KEY (ID))";
        executeSQL(createString);

        String insertString1 =
                "INSERT INTO TEST_STRING VALUES("
                + "'10',"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL)";
        executeSQL(insertString1);

        String insertString2 =
                "INSERT INTO TEST_STRING VALUES("
                + "'11',"
                + "'aa',"
                + "'AA',"
                + "'aaaaaaaaaaaa',"
                + "'ああ',"
                + "'いいいい',"
                + "'ａａ',"
                + "'ＡＡ',"
                + "'ａａａａａａａａ')";
        executeSQL(insertString2);

        String dropNumber = "DROP TABLE TEST_NUMBER";
        executeSQL(dropNumber, true);

        String createNumber =
                "CREATE TABLE TEST_NUMBER ("
                + "ID               CHAR(2) NOT NULL,"
                + "SMALLINT_ITEM    SMALLINT,"
                + "INTEGER_ITEM     INTEGER,"
                + "BIGINT_ITEM      BIGINT,"
                + "DECIMAL_ITEM      DECIMAL(8,2),"
                + "NUMERIC_ITEM     NUMERIC(8,2),"
                + "REAL_ITEM        REAL,"
                + "DOUBLE_ITEM      DOUBLE,"
                + "FLOAT_ITEM       FLOAT,"
                + "PRIMARY KEY (ID))";
        executeSQL(createNumber);

        String insertNumber1 =
                "INSERT INTO TEST_NUMBER VALUES("
                + "'10',"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL)";
        executeSQL(insertNumber1);

        String insertNumber2 =
                "INSERT INTO TEST_NUMBER VALUES("
                + "'11',"
                + "12345,"
                + "123456789,"
                + "123456789012,"
                + "123456.78,"
                + "876543.21,"
                + "1.23456,"
                + "1.23456789012,"
                + "3.45678901234)";
        executeSQL(insertNumber2);

        String insertNumber3 =
                "INSERT INTO TEST_NUMBER VALUES("
                + "'12',"
                + "-12345,"
                + "-123456789,"
                + "-123456789012,"
                + "-123456.78,"
                + "-876543.21,"
                + "-1.23456,"
                + "-1.23456789012,"
                + "-3.45678901234)";
        executeSQL(insertNumber3);

        String dropDateTime = "DROP TABLE TEST_DATETIME";
        executeSQL(dropDateTime, true);

        String createDateTime =
                "CREATE TABLE TEST_DATETIME ("
                + "ID               CHAR(2) NOT NULL,"
                + "DATE_ITEM        DATE,"
                + "TIME_ITEM        TIME,"
                + "TIMESTAMP_ITEM   TIMESTAMP,"
                + "TIMESTAMP0_ITEM  TIMESTAMP(0),"
                + "TIMESTAMP12_ITEM TIMESTAMP(12),"
                + "PRIMARY KEY (ID))";
        executeSQL(createDateTime);

        String insertDateTime1 =
                "INSERT INTO TEST_DATETIME VALUES("
                + "'10',"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL)";
        executeSQL(insertDateTime1);

        String insertDateTime2 =
                "INSERT INTO TEST_DATETIME VALUES("
                + "'11',"
                + "'2016-09-08',"
                + "'12:34:45',"
                + "'2016-09-09 12:34:45.123456',"
                + "'2016-09-10 12:34:45',"
                + "'2016-09-11 12:34:45.123456789012')";
        executeSQL(insertDateTime2);

        /*
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
        executeSQL(create1);

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
        executeSQL(insert1);

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
        executeSQL(insert2);

        String drop2 = "drop table if exists test2";
        executeSQL(drop2);

        String create2 = "create table test2 (c1 bigint unsigned);";
        executeSQL(create2);

        String insert3 = "insert into test2 values(18446744073709551615)";
        executeSQL(insert3);*/
    }

    @Test
    public void testString() throws Exception
    {
        if (enabled) {
            test("/db2/yml/input-string.yml");
            assertEquals(Arrays.asList(
                    "ID,CHAR_ITEM,VARCHAR_ITEM,CLOB_ITEM,GRAPHIC_ITEM,VARGRAPHIC_ITEM,NCHAR_ITEM,NVARCHAR_ITEM,NCLOB_ITEM",
                    "10,,,,,,,,",
                    "11,aa  ,AA,aaaaaaaaaaaa,ああ  ,いいいい,ａａ  ,ＡＡ,ａａａａａａａａ"),
                    read("db2-input000.00.csv"));
        }
    }

    @Test
    public void testNumber() throws Exception
    {
        if (enabled) {
            test("/db2/yml/input-number.yml");
            assertEquals(Arrays.asList(
                    "ID,SMALLINT_ITEM,INTEGER_ITEM,BIGINT_ITEM,DECIMAL_ITEM,NUMERIC_ITEM,REAL_ITEM,DOUBLE_ITEM,FLOAT_ITEM",
                    "10,,,,,,,,",
                    // (double)1.23456f becomes "1.2345600128173828", not "1.23456", because of difference of precision.
                    "11,12345,123456789,123456789012,123456.78,876543.21," + (double)1.23456f + ",1.23456789012,3.45678901234",
                    "12,-12345,-123456789,-123456789012,-123456.78,-876543.21," + (double)-1.23456f + ",-1.23456789012,-3.45678901234"),
                    read("db2-input000.00.csv"));
        }
    }

    @Test
    public void testDateTime() throws Exception
    {
        if (enabled) {
            test("/db2/yml/input-datetime.yml");
            assertEquals(Arrays.asList(
                    "ID,DATE_ITEM,TIME_ITEM,TIMESTAMP_ITEM,TIMESTAMP0_ITEM,TIMESTAMP12_ITEM",
                    "10,,,,,",
                    // precision of embulk timestamp is nano seconds
                    "11,2016/09/07,03-34-45,2016/09/09 03:34:45,2016/09/10 12:34:45,2016/09/11 03:34:45.123456789000"),
                    read("db2-input000.00.csv"));
        }
    }

    @Override
    protected Connection connect() throws SQLException
    {
        return DriverManager.getConnection(String.format(ENGLISH, "jdbc:db2://%s:%d/%s", getHost(), getPort(), getDatabase()),
                getUser(), getPassword());
    }
}
