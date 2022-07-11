package org.embulk.input.sqlserver;

import static org.embulk.input.sqlserver.SQLServerTests.execute;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.nio.file.Path;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.file.LocalFileOutputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class BasicTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/input/sqlserver/test/expect/basic/";

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
            .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
            .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
            .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
            .registerPlugin(FileOutputPlugin.class, "file", LocalFileOutputPlugin.class)
            .registerPlugin(InputPlugin.class, "sqlserver", SQLServerInputPlugin.class)
            .build();

    private ConfigSource baseConfig;

    @Before
    public void setup()
    {
        baseConfig = SQLServerTests.baseConfig();
        execute(readResource("setup.sql")); // setup rows
    }

    @Test
    public void test() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "test_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("test_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testJTDS() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "jtds_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("test_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }
}
