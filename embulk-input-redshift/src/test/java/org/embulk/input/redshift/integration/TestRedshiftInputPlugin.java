package org.embulk.input.redshift.integration;

import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigSource;
import org.embulk.input.RedshiftInputPlugin;
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
import static org.junit.Assume.assumeNotNull;

public class TestRedshiftInputPlugin
    extends AbstractJdbcInputPluginTest
{
    private static String REDSHIFT_HOST;
    private static String REDSHIFT_USER;
    private static String REDSHIFT_PASSWORD;
    private static String REDSHIFT_DATABASE;

    private static final int REDSHIFT_PORT = 5439;
    private static final String REDSHIFT_TABLE = "embulk_input_mysql_test";

    private static Connection conn;

    /*
     * This test case requires environment variables:
     *   REDSHIFT_TEST_HOST
     *   REDSHIFT_TEST_USER
     *   REDSHIFT_TEST_PASSWORD
     *   REDSHIFT_TEST_DATABASE
     * If the variables not set, the test case is skipped.
     */
    @BeforeClass
    public static void initializeConstantVariables()
    {
        REDSHIFT_HOST = System.getenv("REDSHIFT_TEST_HOST");
        REDSHIFT_USER = System.getenv("REDSHIFT_TEST_USER");
        REDSHIFT_PASSWORD = System.getenv("REDSHIFT_TEST_PASSWORD");
        REDSHIFT_DATABASE = System.getenv("REDSHIFT_TEST_DATABASE");
        assumeNotNull(REDSHIFT_HOST, REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_DATABASE);

        // get connection
        createConnection();

        // ensure create table
        ensureCreateEmptyTable();

        // insert sample records
        insertSampleRecords();
    }

    private static void createConnection()
    {
        conn = setConnection("jdbc:postgresql://" + REDSHIFT_HOST + ":" + REDSHIFT_PORT + "/" + REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD);
    }

    private static void ensureCreateEmptyTable()
    {
        dropTable(conn, REDSHIFT_TABLE); // TODO retry

        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        builder.add("v_bool").add("boolean null")
                .add("v_int").add("int null")
                .add("v_bigint").add("bigint null")
                .add("v_float").add("float null")
                .add("v_varchar").add("varchar(32) null")
                .add("v_date").add("date null")
                .add("v_timestamp").add("timestamp null");
        createTable(conn, REDSHIFT_TABLE, builder.build()); // TODO retry
    }

    private static void insertSampleRecords()
    {
        insertRecord(conn, REDSHIFT_TABLE, ImmutableList.<String>of(
                "true", "2147483647", "9223372036854775807", "0.1", "'redshift!'", "'2015-09-04'", "'2015-09-04 21:00:00'"
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
                .add("v_timestamp", Types.TIMESTAMP)
                .build();
    }

    @Override
    public ConfigSource buildConfigSource(ConfigSource config)
    {
        return config.set("type", "redshift")
                .set("host", REDSHIFT_HOST)
                .set("user", REDSHIFT_USER)
                .set("password", REDSHIFT_PASSWORD)
                .set("database", REDSHIFT_DATABASE)
                .set("fetch_rows", 100);
    }

    @Override
    public Class<RedshiftInputPlugin> getInputPluginClass()
    {
        return RedshiftInputPlugin.class;
    }

    @Test
    public void useQuery()
    {
        ConfigSource config = this.config.deepCopy().set("query", "SELECT * FROM " + REDSHIFT_TABLE + ";");
        plugin.transaction(config, new Control());
        assertRecords(output, schema);
    }


    private void assertRecords(MockPageOutput output, Schema schema)
    {
        List<Object[]> records = Pages.toObjects(schema, output.pages);
        assertEquals(1, records.size());

        {
            Object[] record = records.get(0);
            assertEquals(true, record[0]);
            assertEquals(2147483647L, record[1]);
            assertEquals(9223372036854775807L, record[2]);
            assertEquals(0.1, (Double) record[3], 0.00001);
            assertEquals("redshift!", record[4]);
            assertEquals("2015-09-04 07:00:00 UTC", record[5].toString()); // timestamp
            assertEquals("2015-09-05 04:00:00 UTC", record[6].toString()); // timestamp
        }
    }
}
