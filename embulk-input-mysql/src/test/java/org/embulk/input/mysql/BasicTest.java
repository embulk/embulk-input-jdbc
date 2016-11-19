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
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic_test/config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic_test/expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic_test/expected.diff")));
    }

    @Test
    public void testString() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic_test_string/config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic_test_string/expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic_test_string/expected.diff")));
    }

    @Test
    public void testBoolean() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic_test_boolean/config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic_test_boolean/expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic_test_boolean/expected.diff")));
    }

    @Test
    public void testLong() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic_test_long/config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic_test_long/expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic_test_long/expected.diff")));
    }

    @Test
    public void testDouble() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic_test_double/config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic_test_double/expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic_test_double/expected.diff")));
    }

    @Test
    public void testTimestamp1() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic_test_timestamp1/config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic_test_timestamp1/expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic_test_timestamp1/expected.diff")));
    }

    @Test
    public void testTimestamp2() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic_test_timestamp2/config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic_test_timestamp2/expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic_test_timestamp2/expected.diff")));
    }

    @Test
    public void testTimestamp3() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic_test_timestamp3/config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic_test_timestamp3/expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic_test_timestamp3/expected.diff")));
    }

    @Test
    public void testValueTypeString() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic_test_value_type_string/config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic_test_value_type_string/expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic_test_value_type_string/expected.diff")));
    }

    @Test
    public void testValueTypeDecimal() throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result1 = embulk.runInput(baseConfig.merge(embulk.loadYamlResource("expect/basic_test_value_type_decimal/config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("expect/basic_test_value_type_decimal/expected.csv")));
        assertThat(result1.getConfigDiff(), is((ConfigDiff) embulk.loadYamlResource("expect/basic_test_value_type_decimal/expected.diff")));
    }
}
