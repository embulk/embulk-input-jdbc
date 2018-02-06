package org.embulk.input.postgresql;

import org.embulk.config.ConfigSource;
import org.embulk.input.PostgreSQLInputPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;

import static org.embulk.input.postgresql.PostgreSQLTests.execute;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ArrayTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/input/postgresql/test/expect/array/";

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
    public void loadAsStringByDefault() throws Exception
    {
        execute(readResource("setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "as_string.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("expected_string.csv")));
    }

    @Test
    public void loadAsJson() throws Exception
    {
        execute(readResource("setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        embulk.runInput(
                baseConfig.merge(loadYamlResource(embulk, "as_json.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("expected_json.csv")));
    }
}
