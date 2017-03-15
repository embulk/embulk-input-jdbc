package org.embulk.input.oracle;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.input.OracleInputPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.embulk.test.TestingEmbulk.RunResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;

import static org.embulk.input.oracle.OracleTests.execute;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class IncrementalTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/input/oracle/test/expect/incremental/";

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
        .registerPlugin(InputPlugin.class, "oracle", OracleInputPlugin.class)
        .build();

    private ConfigSource baseConfig;

    @Before
    public void setup()
    {
        baseConfig = OracleTests.baseConfig();
    }

    @Test
    public void simpleInt() throws Exception
    {
        // setup first rows
        execute(embulk, readResource("int/setup.sql"));

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
        execute(embulk, readResource("int/insert_more.sql"));

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
    public void simpleDate() throws Exception
    {
        // setup first rows
        execute(embulk, readResource("date/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "date/config_1.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("date/expected_1.csv")));
        assertThat(
                result1.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "date/expected_1.diff")));

        // insert more rows
        execute(embulk, readResource("date/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "date/config_2.yml")),
                out2);
        assertThat(
                readSortedFile(out2),
                is(readResource("date/expected_2.csv")));
        assertThat(
                result2.getConfigDiff(),
                is((ConfigDiff) loadYamlResource(embulk, "date/expected_2.diff")));
    }

    @Test
    public void simpleTimestamp() throws Exception
    {
        // setup first rows
        execute(embulk, readResource("timestamp/setup.sql"));

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
        execute(embulk, readResource("timestamp/insert_more.sql"));

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

}
