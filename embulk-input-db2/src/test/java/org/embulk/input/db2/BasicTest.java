package org.embulk.input.db2;

import static org.embulk.input.db2.DB2Tests.execute;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.nio.file.Path;
import java.util.TimeZone;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.input.DB2InputPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class BasicTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/input/db2/test/expect/basic/";

    private static ConfigSource loadYamlResource(TestingEmbulk embulk, String fileName)
    {
        return embulk.loadYamlResource(BASIC_RESOURCE_PATH + fileName);
    }

    private static String readResource(String fileName)
    {
        return EmbulkTests.readResource(BASIC_RESOURCE_PATH + fileName);
    }

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(InputPlugin.class, "db2", DB2InputPlugin.class)
            .build();

    private ConfigSource baseConfig;

    @Before
    public void setup() throws Exception
    {
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki")); // +200
        baseConfig = DB2Tests.baseConfig();
        execute(BASIC_RESOURCE_PATH + "setup.sql"); // setup rows
    }

    @Test
    public void testString() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "test_string_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("test_string_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_string_expected.diff")));
    }

    @Test
    public void testNumber() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "test_number_config.yml")), out1);
        // (double)1.23456f becomes "1.2345600128173828", not "1.23456", because of difference of precision.
        assertThat(readSortedFile(out1), is(readResource("test_number_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_number_expected.diff")));
    }

    @Test
    public void testDatetime() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "test_datetime_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("test_datetime_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_datetime_expected.diff")));
    }
}
