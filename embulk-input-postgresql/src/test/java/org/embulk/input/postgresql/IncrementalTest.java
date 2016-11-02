package org.embulk.input.postgresql;

import org.embulk.input.PostgreSQLInputPlugin;
import org.embulk.test.EmbulkTestingEmbed;
import org.embulk.test.EmbulkTestingEmbed.RunResult;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.embulk.spi.InputPlugin;

import java.nio.file.Path;
import org.embulk.config.Config;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.embulk.input.postgresql.PostgreSQLTests.execute;
import static org.embulk.test.EmbulkTests.readResource;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class IncrementalTest
{
    private EmbulkTestingEmbed embulk = EmbulkTestingEmbed.builder()
        .registerPlugin(InputPlugin.class, "postgresql", PostgreSQLInputPlugin.class)
        .build();

    ConfigSource baseConfig;

    @Before
    public void setup()
    {
        baseConfig = PostgreSQLTests.baseConfig();
    }

    @Test
    public void simpleInt() throws Exception
    {
        // setup first rows
        execute(readResource("expect/incremental/int/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(
                baseConfig.merge(embulk.loadYamlResource("expect/incremental/int/config_1.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("expect/incremental/int/expected_1.csv")));
        assertThat(
                result1.getConfigDiff(),
                is((ConfigDiff) embulk.loadYamlResource("expect/incremental/int/expected_1.diff")));

        // insert more rows
        execute(readResource("expect/incremental/int/insert_more.sql"));

        Path out2 = embulk.createTempFile("csv");
        RunResult result2 = embulk.runInput(
                baseConfig.merge(embulk.loadYamlResource("expect/incremental/int/config_2.yml")),
                out2);
        assertThat(
                readSortedFile(out2),
                is(readResource("expect/incremental/int/expected_2.csv")));
        assertThat(
                result2.getConfigDiff(),
                is((ConfigDiff) embulk.loadYamlResource("expect/incremental/int/expected_2.diff")));
    }
}
