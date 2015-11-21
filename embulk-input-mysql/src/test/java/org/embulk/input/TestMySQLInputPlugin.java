package org.embulk.input;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Pages;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeNotNull;

public class TestMySQLInputPlugin
    extends AbstractJdbcInputPluginTest
{
    private static String MYSQL_HOST;
    private static String MYSQL_USER;
    private static String MYSQL_PASSWORD;
    private static String MYSQL_DATABASE;

    private static final String MYSQL_TABLE = "embulk_input_mysql_test";

    private static Connection conn;

    /*
     * This test case requires environment variables:
     *   MYSQL_TEST_HOST
     *   MYSQL_TEST_USER
     *   MYSQL_TEST_PASSWORD
     *   MYSQL_TEST_DATABASE
     * If the variables not set, the test case is skipped.
     */
    @BeforeClass
    public static void initializeConstantVariables()
    {
        MYSQL_HOST = System.getenv("MYSQL_TEST_HOST");
        MYSQL_USER = System.getenv("MYSQL_TEST_USER");
        MYSQL_PASSWORD = System.getenv("MYSQL_TEST_PASSWORD");
        MYSQL_DATABASE = System.getenv("MYSQL_TEST_DATABASE");
        assumeNotNull(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE);

        // get connection
        createConnection();

        // ensure create table
        ensureCreateEmptyTable();

        // insert sample records
        insertSampleRecords();
    }

    private static void createConnection()
    {
        conn = setConnection("jdbc:mysql://" + MYSQL_HOST + "/" + MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD);
    }

    private static void ensureCreateEmptyTable()
    {
        dropTable(conn, MYSQL_TABLE); // TODO retry

        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        builder.add("v_bool").add("boolean null")
                .add("v_int").add("int null")
                .add("v_bigint").add("bigint null")
                .add("v_float").add("float null")
                .add("v_double").add("double null")
                .add("v_decimal").add("decimal(4, 2) null")
                .add("v_varchar").add("varchar(32) null")
                .add("v_date").add("date null")
                .add("v_time").add("time null")
                .add("v_datetime").add("datetime null")
                .add("v_timestamp").add("timestamp null");
        createTable(conn, MYSQL_TABLE, builder.build()); // TODO retry
    }

    private static void insertSampleRecords()
    {
        insertRecord(conn, MYSQL_TABLE, ImmutableList.<String>of(
                "true", "2147483647", "9223372036854775807", "0.1", "1.1", "10.11", "'mysql!'", "'2015-09-04'", "'21:00:00'", "'2015-09-04 21:00:00'", "'2015-09-04 21:00:00'"
        )); // TODO retry

        insertRecord(conn, MYSQL_TABLE, ImmutableList.<String>of(
                "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"
        )); // TODO retry
    }

    @Override
    public Schema buildSchema() {
        return new Schema.Builder()
                .add("v_bool", Types.BOOLEAN)
                .add("v_int", Types.LONG)
                .add("v_bigint", Types.LONG)
                .add("v_float", Types.DOUBLE)
                .add("v_double", Types.DOUBLE)
                .add("v_decimal", Types.DOUBLE)
                .add("v_varchar", Types.STRING)
                .add("v_date", Types.TIMESTAMP)
                .add("v_time", Types.TIMESTAMP)
                .add("v_datetime", Types.TIMESTAMP)
                .add("v_timestamp", Types.TIMESTAMP)
                .build();
    }

    @Override
    public ConfigSource buildConfigSource(ConfigSource config)
    {
        return config.set("type", "mysql")
                     .set("host", MYSQL_HOST)
                     .set("user", MYSQL_USER)
                     .set("password", MYSQL_PASSWORD)
                     .set("database", MYSQL_DATABASE);
    }

    @Override
    public Class<MySQLInputPlugin> getInputPluginClass()
    {
        return MySQLInputPlugin.class;
    }

    @Test
    public void useQuery()
    {
        ConfigSource config = this.config.deepCopy().set("query", "SELECT * FROM " + MYSQL_TABLE + ";");
        plugin.transaction(config, new Control());
        assertRecords(output, schema);
    }

    @Test
    public void useSelectAndTable()
    {
        ConfigSource config = this.config.deepCopy().set("select", "*").set("table", MYSQL_TABLE);
        plugin.transaction(config, new Control());
        assertRecords(output, schema);
    }

    private void assertRecords(MockPageOutput output, Schema schema)
    {
        List<Object[]> records = Pages.toObjects(schema, output.pages);
        assertEquals(2, records.size());

        {
            Object[] record = records.get(0);
            assertEquals(true, record[0]);
            assertEquals(2147483647L, record[1]);
            assertEquals(9223372036854775807L, record[2]);
            assertEquals(0.1, (Double) record[3], 0.00001);
            assertEquals(1.1, (Double) record[4], 0.00001);
            assertEquals(10.11, (Double) record[5], 0.00001);
            assertEquals("mysql!", record[6]);
            assertEquals("2015-09-04 07:00:00 UTC", record[7].toString()); // timestamp
            assertEquals("1970-01-02 05:00:00 UTC", record[8].toString()); // timestamp
            assertEquals("2015-09-05 04:00:00 UTC", record[9].toString()); // timestamp
            assertEquals("2015-09-05 04:00:00 UTC", record[10].toString()); // timestamp
        }

        {
            Object[] record = records.get(1);
            for (Object r : record) {
                assertNull(r);
            }
        }
    }

    @Test
    public void useColumnOptionsType()
    {
        schema = new Schema.Builder()
                .add("v_bool1", Types.BOOLEAN).add("v_bool2", Types.DOUBLE).add("v_bool3", Types.STRING).add("v_bool4", Types.LONG)
                .add("v_int1", Types.BOOLEAN).add("v_int2", Types.DOUBLE).add("v_int3", Types.STRING).add("v_int4", Types.LONG)
                .add("v_float1", Types.BOOLEAN).add("v_float2", Types.DOUBLE).add("v_float3", Types.STRING).add("v_float4", Types.LONG)
                .add("v_double1", Types.BOOLEAN).add("v_double2", Types.DOUBLE).add("v_double3", Types.STRING).add("v_double4", Types.LONG)
                .add("v_decimal1", Types.BOOLEAN).add("v_decimal2", Types.DOUBLE).add("v_decimal3", Types.STRING).add("v_decimal4", Types.LONG)
                .add("v_varchar1", Types.DOUBLE).add("v_varchar2", Types.STRING).add("v_varchar3", Types.LONG)
                .build();
        ConfigSource config = this.config.deepCopy()
                .set("select", "v_bool as v_bool1, v_bool as v_bool2, v_bool as v_bool3, v_bool as v_bool4, " +
                        "v_int as v_int1, v_int as v_int2, v_int as v_int3, v_int as v_int4, " +
                        "v_float as v_float1, v_float as v_float2, v_float as v_float3, v_float as v_float4, " +
                        "v_double as v_double1, v_double as v_double2, v_double as v_double3, v_double as v_double4, " +
                        "v_decimal as v_decimal1, v_decimal as v_decimal2, v_decimal as v_decimal3, v_decimal as v_decimal4, " +
                        "v_varchar as v_varchar1, v_varchar as v_varchar2, v_varchar as v_varchar3")
                .set("table", MYSQL_TABLE)
                .set("column_options", getColumnOptionTypes(
                        "v_bool1", "boolean", "v_bool2", "double", "v_bool3", "string", "v_bool4", "long",
                        "v_int1", "boolean", "v_int2", "double", "v_int3", "string", "v_int4", "long",
                        "v_float1", "boolean", "v_float2", "double", "v_float3", "string", "v_float4", "long",
                        "v_double1", "boolean", "v_double2", "double", "v_double3", "string", "v_double4", "long",
                        "v_decimal1", "boolean", "v_decimal2", "double", "v_decimal3", "string", "v_decimal4", "long",
                        "v_varchar1", "double", "v_varchar2", "string", "v_varchar3", "long"
                ));
        plugin.transaction(config, new Control());

        List<Object[]> records = Pages.toObjects(schema, output.pages);
        assertEquals(2, records.size());

        {
            Object[] record = records.get(0);

            assertEquals(true, record[0]);
            assertEquals(1.0, (double) record[1], 0.00001);
            assertEquals("true", record[2]);
            assertEquals(1L, (long) record[3]);

            assertEquals(true, record[4]);
            assertEquals(2147483647, (double) record[5], 0.00001);
            assertEquals("2147483647", record[6]);
            assertEquals(2147483647, (long) record[7]);

            assertEquals(true, record[8]);
            assertEquals(0.1, (double) record[9], 0.00001);
            assertEquals("0.1", record[10]);
            assertEquals(0, (long) record[11]);

            assertEquals(true, record[12]);
            assertEquals(1.1, (double) record[13], 0.00001);
            assertEquals("1.1", record[14]);
            assertEquals(1, (long) record[15]);

            assertEquals(true, record[16]);
            assertEquals(10.11, (double) record[17], 0.00001);
            assertEquals("10.11", record[18]);
            assertEquals(10, (long) record[19]);

            assertNull(record[20]);
            assertEquals("mysql!", record[21]);
            assertNull(record[22]);
        }

        {
            Object[] record = records.get(1);
            for (Object r : record) {
                assertNull(r);
            }
        }
    }

    private ImmutableMap<String, Object> getColumnOptionTypes(String... nameAndTypes)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        for (int i = 0; i < nameAndTypes.length; i += 2) {
            builder.put(nameAndTypes[i], ImmutableMap.of("type", nameAndTypes[i + 1]));
        }
        return builder.build();
    }

    @Test(expected=ConfigException.class)
    public void cannotUseQueryAndSelect()
    {
        ConfigSource config = this.config.deepCopy()
                .set("query", "SELECT * FROM " + MYSQL_TABLE + ";").set("select", "*").set("table", MYSQL_TABLE);
        plugin.transaction(config, new Control());
    }

}
