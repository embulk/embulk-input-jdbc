package org.embulk.input.postgresql;

import java.nio.file.Path;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.input.PostgreSQLInputPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.test.TestingEmbulk.RunResult;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.input.postgresql.PostgreSQLTests.execute;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class IncrementalTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/input/postgresql/test/expect/incremental/";

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
        .registerPlugin(InputPlugin.class, "postgresql", PostgreSQLInputPlugin.class)
        .build();

    private ConfigSource baseConfig;

    @Before
    public void setup()
    {
        baseConfig = PostgreSQLTests.baseConfig();
    }

    @Test
    public void simpleInt() throws Exception
    {
        // setup first rows
        execute(readResource("int/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "int/config_1.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("int/expected_1.csv")));
        assertThat(
                result1.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "int/expected_1.diff")));

        // insert more rows
        execute(readResource("int/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "int/config_2.yml")),
                out2);
        assertThat(
                readSortedFile(out2),
                is(readResource("int/expected_2.csv")));
        assertThat(
                result2.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "int/expected_2.diff")));
    }

    @Test
    public void simpleChar() throws Exception
    {
        // setup first rows
        execute(readResource("char/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "char/config_1.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("char/expected_1.csv")));
        assertThat(
                result1.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "char/expected_1.diff")));

        // insert more rows
        execute(readResource("char/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "char/config_2.yml")),
                out2);
        assertThat(
                readSortedFile(out2),
                is(readResource("char/expected_2.csv")));
        assertThat(
                result2.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "char/expected_2.diff")));
    }

    @Test
    public void simpleTimestampWithoutTimeZone() throws Exception
    {
        // setup first rows
        execute(readResource("timestamp/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "timestamp/config_1.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("timestamp/expected_1.csv")));
        assertThat(
                result1.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "timestamp/expected_1.diff")));

        // insert more rows
        execute(readResource("timestamp/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "timestamp/config_2.yml")),
                out2);
        assertThat(
                readSortedFile(out2),
                is(readResource("timestamp/expected_2.csv")));
        assertThat(
                result2.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "timestamp/expected_2.diff")));
    }

    @Test
    public void simpleTimestampWithTimeZone() throws Exception
    {
        // setup first rows
        execute(readResource("timestamptz/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "timestamptz/config_1.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("timestamptz/expected_1.csv")));
        assertThat(
                result1.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "timestamptz/expected_1.diff")));

        // insert more rows
        execute(readResource("timestamptz/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "timestamptz/config_2.yml")),
                out2);
        assertThat(
                readSortedFile(out2),
                is(readResource("timestamptz/expected_2.csv")));
        assertThat(
                result2.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "timestamptz/expected_2.diff")));
    }

    @Test
    public void simpleQueryWithPlaceholder() throws Exception
    {
        // setup first rows
        execute(readResource("query/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "query/config_1.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("query/expected_1.csv")));
        assertThat(
                result1.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "query/expected_1.diff")));

        // insert more rows
        execute(readResource("query/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "query/config_2.yml")),
                out2);
        assertThat(
                readSortedFile(out2),
                is(readResource("query/expected_2.csv")));
        assertThat(
                result2.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "query/expected_2.diff")));
    }

    @Test
    public void simpleQueryWithPlaceholderAndMultiColumns() throws Exception
    {
        // setup first rows
        execute(readResource("query/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "query/multi_columns_config_1.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("query/multi_columns_expected_1.csv")));
        assertThat(
                result1.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "query/multi_columns_expected_1.diff")));

        // insert more rows
        execute(readResource("query/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "query/multi_columns_config_2.yml")),
                out2);
        assertThat(
                readSortedFile(out2),
                is(readResource("query/multi_columns_expected_2.csv")));
        assertThat(
                result2.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "query/multi_columns_expected_2.diff")));
    }
}
