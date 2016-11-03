package org.embulk.input.postgresql;

import java.nio.file.Path;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.input.PostgreSQLInputPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.test.TestingEmbulk.RunResult;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import static org.embulk.input.postgresql.PostgreSQLTests.execute;
import static org.embulk.test.EmbulkTests.readResource;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class HstoreTest
{
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
        execute(readResource("expect/hstore/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(
                baseConfig.merge(embulk.loadYamlResource("expect/hstore/as_string.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("expect/hstore/expected_string.csv")));
    }

    @Test
    public void loadAsJson() throws Exception
    {
        execute(readResource("expect/hstore/setup.sql"));

        Path out1 = embulk.createTempFile("csv");
        RunResult result1 = embulk.runInput(
                baseConfig.merge(embulk.loadYamlResource("expect/hstore/as_json.yml")),
                out1);
        assertThat(
                readSortedFile(out1),
                is(readResource("expect/hstore/expected_json.csv")));
    }
}
