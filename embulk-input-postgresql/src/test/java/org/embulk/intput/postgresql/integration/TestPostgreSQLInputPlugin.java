package org.embulk.intput.postgresql.integration;

import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigSource;
import org.embulk.input.PostgreSQLInputPlugin;
import org.embulk.input.jdbc.integration.AbstractJdbcInputPluginTest;
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

public class TestPostgreSQLInputPlugin
    extends AbstractJdbcInputPluginTest
{
    private static String POSTGRESQL_HOST;
    private static String POSTGRESQL_USER;
    private static String POSTGRESQL_PASSWORD;
    private static String POSTGRESQL_DATABASE;

    private static final String POSTGRESQL_TABLE = "embulk_input_postgresql_test";

    private static Connection conn;

    /*
     * This test case requires environment variables:
     *   POSTGRESQL_TEST_HOST
     *   POSTGRESQL_TEST_USER
     *   POSTGRESQL_TEST_PASSWORD
     *   POSTGRESQL_TEST_DATABASE
     * If the variables not set, the test case is skipped.
     */
    @BeforeClass
    public static void initializeConstantVariables()
    {
        POSTGRESQL_HOST = System.getenv("POSTGRESQL_TEST_HOST");
        POSTGRESQL_USER = System.getenv("POSTGRESQL_TEST_USER");
        POSTGRESQL_PASSWORD = System.getenv("POSTGRESQL_TEST_PASSWORD");
        POSTGRESQL_DATABASE = System.getenv("POSTGRESQL_TEST_DATABASE");
        assumeNotNull(POSTGRESQL_HOST, POSTGRESQL_USER, POSTGRESQL_PASSWORD, POSTGRESQL_DATABASE);

        // get connection
        createConnection();

        // ensure create table
        ensureCreateEmptyTable();

        // insert sample records
        insertSampleRecords();
    }

    private static void createConnection()
    {
        conn = setConnection("jdbc:postgresql://" + POSTGRESQL_HOST + "/" + POSTGRESQL_DATABASE, POSTGRESQL_USER, POSTGRESQL_PASSWORD);
    }

    private static void ensureCreateEmptyTable()
    {
        dropTable(conn, POSTGRESQL_TABLE); // TODO retry

        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        builder.add("v_bool").add("boolean null")
                .add("v_int").add("int null")
                .add("v_bigint").add("bigint null")
                .add("v_float").add("float null")
                .add("v_varchar").add("varchar(32) null")
                .add("v_date").add("date null")
                .add("v_time").add("time null")
                .add("v_timestamp").add("timestamp null");
        createTable(conn, POSTGRESQL_TABLE, builder.build()); // TODO retry
    }

    private static void insertSampleRecords()
    {
        insertRecord(conn, POSTGRESQL_TABLE, ImmutableList.<String>of(
                "true", "2147483647", "9223372036854775807", "0.1", "'postgresql!'", "'2015-09-04'", "'21:00:00'", "'2015-09-04 21:00:00'"
        )); // TODO retry

        insertRecord(conn, POSTGRESQL_TABLE, ImmutableList.<String>of(
                "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"
        )); // TODO retry
    }

    @Override
    public Schema buildSchema() {
        return new Schema.Builder()
                .add("v_bool", Types.BOOLEAN)
                .add("v_int", Types.LONG)
                .add("v_bigint", Types.LONG)
                .add("v_float", Types.DOUBLE)
                .add("v_varchar", Types.STRING)
                .add("v_date", Types.TIMESTAMP)
                .add("v_time", Types.TIMESTAMP)
                .add("v_timestamp", Types.TIMESTAMP)
                .build();
    }

    @Override
    public ConfigSource buildConfigSource(ConfigSource config)
    {
        return config.set("type", "postgresql")
                .set("host", POSTGRESQL_HOST)
                .set("user", POSTGRESQL_USER)
                .set("password", POSTGRESQL_PASSWORD)
                .set("database", POSTGRESQL_DATABASE);
    }

    @Override
    public Class<PostgreSQLInputPlugin> getInputPluginClass()
    {
        return PostgreSQLInputPlugin.class;
    }

    @Test
    public void useQuery()
    {
        ConfigSource config = this.config.deepCopy().set("query", "SELECT * FROM " + POSTGRESQL_TABLE + ";");
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
            assertEquals("postgresql!", record[4]);
            assertEquals("2015-09-04 07:00:00 UTC", record[5].toString()); // timestamp
            assertEquals("1970-01-02 05:00:00 UTC", record[6].toString()); // timestamp
            assertEquals("2015-09-05 04:00:00 UTC", record[7].toString()); // timestamp
        }

        {
            Object[] record = records.get(1);
            for (Object r : record) {
                assertNull(r);
            }
        }
    }
}