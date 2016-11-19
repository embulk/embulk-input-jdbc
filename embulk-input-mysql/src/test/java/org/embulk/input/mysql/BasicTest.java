package org.embulk.input.mysql;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.input.MySQLInputPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;

import static org.embulk.input.mysql.MySQLTests.execute;
import static org.embulk.test.EmbulkTests.readResource;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class BasicTest
{
    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(InputPlugin.class, "mysql", MySQLInputPlugin.class)
            .build();

    private ConfigSource baseConfig;

    @Before
    public void setup()
    {
        baseConfig = MySQLTests.baseConfig();
        execute(readResource("expect/basic/setup.sql")); // setup rows
    }

    @Test
    public void test() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic/test_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic/test_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic/test_expected.diff")));
    }

    @Test
    public void testString() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic/test_string_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic/test_string_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic/test_string_expected.diff")));
    }

    @Test
    public void testBoolean() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic/test_boolean_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic/test_boolean_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic/test_boolean_expected.diff")));
    }

    @Test
    public void testLong() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic/test_long_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic/test_long_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic/test_long_expected.diff")));
    }

    @Test
    public void testDouble() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic/test_double_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic/test_double_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic/test_double_expected.diff")));
    }

    @Test
    public void testTimestamp1() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic/test_timestamp1_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic/test_timestamp1_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic/test_timestamp1_expected.diff")));
    }

    @Test
    public void testTimestamp2() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic/test_timestamp2_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic/test_timestamp2_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic/test_timestamp2_expected.diff")));
    }

    @Test
    public void testTimestamp3() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic/test_timestamp3_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic/test_timestamp3_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic/test_timestamp3_expected.diff")));
    }

    @Test
    public void testValueTypeString() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic/test_valuetype_string_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic/test_valuetype_string_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic/test_valuetype_string_expected.diff")));
    }

    @Test
    public void testValueTypeDecimal() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic/test_valuetype_decimal_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic/test_valuetype_decimal_expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic/test_valuetype_decimal_expected.diff")));
    }
}
