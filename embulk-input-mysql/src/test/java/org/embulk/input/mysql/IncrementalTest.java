package org.embulk.input.mysql;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.input.MySQLInputPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.embulk.test.TestingEmbulk.RunResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;

import static org.embulk.input.mysql.MySQLTests.execute;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class IncrementalTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/input/mysql/test/expect/incremental/";

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
            .registerPlugin(InputPlugin.class, "mysql", MySQLInputPlugin.class)
            .build();

    private ConfigSource baseConfig;

    @Before
    public void setup()
    {
        baseConfig = MySQLTests.baseConfig();
    }

    @Test
    public void testInt() throws Exception
    {
        // setup first rows
        execute(readResource("int/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "int/config_1.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("int/expected_1.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "int/expected_1.diff")));

        // insert more rows
        execute(readResource("int/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "int/config_2.yml")), out2);
        assertThat(readSortedFile(out2), is(readResource("int/expected_2.csv")));
        assertThat(result2.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "int/expected_2.diff")));
    }

    @Test
    public void testDateTime() throws Exception
    {
        // setup first rows
        execute(readResource("dt/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "dt/config_1.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("dt/expected_1.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "dt/expected_1.diff")));

        // insert more rows
        execute(readResource("dt/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "dt/config_2.yml")), out2);
        assertThat(readSortedFile(out2), is(readResource("dt/expected_2.csv")));
        assertThat(result2.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "dt/expected_2.diff")));
    }

    @Test
    public void testTimestamp() throws Exception
    {
        // setup first rows
        execute(readResource("ts/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "ts/config_1.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("ts/expected_1.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "ts/expected_1.diff")));

        // insert more rows
        execute(readResource("ts/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "ts/config_2.yml")), out2);
        assertThat(readSortedFile(out2), is(readResource("ts/expected_2.csv")));
        assertThat(result2.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "ts/expected_2.diff")));
    }

    @Test
    public void testQueryWithPlaceholder() throws Exception
    {
        // setup first rows
        execute(readResource("query/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "query/config_1.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("query/expected_1.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "query/expected_1.diff")));

        // insert more rows
        execute(readResource("query/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "query/config_2.yml")), out2);
        assertThat(readSortedFile(out2), is(readResource("query/expected_2.csv")));
        assertThat(result2.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "query/expected_2.diff")));
    }

    @Test
    public void testQueryWithPlaceholderAndMultiColumns() throws Exception
    {
        // setup first rows
        execute(readResource("query/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "query/multi_columns_config_1.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("query/multi_columns_expected_1.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "query/multi_columns_expected_1.diff")));

        // insert more rows
        execute(readResource("query/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "query/multi_columns_config_2.yml")), out2);
        assertThat(readSortedFile(out2), is(readResource("query/multi_columns_expected_2.csv")));
        assertThat(result2.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "query/multi_columns_expected_2.diff")));
    }
}
