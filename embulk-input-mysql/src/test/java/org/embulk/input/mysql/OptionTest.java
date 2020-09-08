package org.embulk.input.mysql;

import org.embulk.config.ConfigSource;
import org.embulk.input.MySQLInputPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;

import static org.embulk.input.mysql.MySQLTests.execute;
import static org.embulk.test.EmbulkTests.readFile;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class OptionTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/input/mysql/test/expect/option/";

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
        execute(readResource("setup.sql")); // setup rows
    }

    @Test
    public void testBeforeSetup() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "before_setup.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("before_setup_expected.csv")));
    }

    @Test
    public void testBeforeSelect() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "before_select.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("before_select_expected.csv")));
    }

    @Test
    public void testAfterSelect() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "after_select.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expected1.csv")));

        baseConfig = MySQLTests.baseConfig();
        Path out2 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result2 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "input.yml")), out2);
        assertThat(readSortedFile(out2), is(readResource("after_select_expected.csv")));
    }

    @Test
    public void testColumnOptions() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "column_options.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("column_options_expected.csv")));
    }

    @Test
    public void testDefaultColumnOptions() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "default_column_options.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("default_column_options_expected.csv")));
    }

    @Test
    public void testOrderBy() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "order_by.yml")), out1);
        assertThat(readFile(out1), is(readResource("order_by_expected.csv")));
    }

    @Test
    public void testOrderByAsc() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "order_by_asc.yml")), out1);
        assertThat(readFile(out1), is(readResource("order_by_asc_expected.csv")));
    }

    @Test
    public void testOrderByDesc() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "order_by_desc.yml")), out1);
        assertThat(readFile(out1), is(readResource("order_by_desc_expected.csv")));
    }

}
