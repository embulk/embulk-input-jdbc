package org.embulk.input;

import static java.util.Locale.ENGLISH;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.embulk.config.ConfigException;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.tester.EmbulkPluginTester;
import org.embulk.input.tester.EmbulkPluginTester.PluginDefinition;
import org.yaml.snakeyaml.Yaml;

import com.google.common.io.Files;

public abstract class AbstractJdbcInputPluginTest
{
    private static final String CONFIG_FILE_NAME = "tests.yml";

    protected boolean enabled;
    // TODO:destroy EmbulkPluginTester after test
    protected EmbulkPluginTester tester = new EmbulkPluginTester();
    private String pluginName;
    private Map<String, ?> testConfigurations;

    protected AbstractJdbcInputPluginTest()
    {
        try {
            prepare();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void prepare() throws SQLException;


    private Map<String, ?> getTestConfigs()
    {
        if (testConfigurations == null) {
            for (PluginDefinition pluginDefinition : tester.getPlugins()) {
                if (AbstractJdbcInputPlugin.class.isAssignableFrom(pluginDefinition.impl)) {
                    pluginName = pluginDefinition.name;
                    break;
                }
            }

            Yaml yaml = new Yaml();
            File configFile = new File(CONFIG_FILE_NAME);
            if (!configFile.exists()) {
                configFile = new File("../" + CONFIG_FILE_NAME);
                if (!configFile.exists()) {
                    throw new ConfigException(String.format(ENGLISH, "\"%s\" doesn't exist.",
                            CONFIG_FILE_NAME));
                }
            }

            try {
                InputStreamReader reader = new InputStreamReader(new FileInputStream(configFile), Charset.forName("UTF8"));
                try {
                    Map<String, ?> allTestConfigs = (Map<String, ?>)yaml.load(reader);
                    if (!allTestConfigs.containsKey(pluginName)) {
                        throw new ConfigException(String.format(ENGLISH, "\"%s\" doesn't contain \"%s\" element.",
                                CONFIG_FILE_NAME, pluginName));
                    }
                    testConfigurations = (Map<String, ?>)allTestConfigs.get(pluginName);
                } finally {
                    reader.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return testConfigurations;
    }

    protected Object getTestConfig(String name, boolean required)
    {
        Map<String, ?> testConfigs = getTestConfigs();
        if (!testConfigs.containsKey(name)) {
            if (required) {
                throw new ConfigException(String.format(ENGLISH, "\"%s\" element in \"%s\" doesn't contain \"%s\" element.",
                        pluginName, CONFIG_FILE_NAME, name));
            }
            return null;
        }
        return testConfigs.get(name);
    }

    protected Object getTestConfig(String name)
    {
        return getTestConfig(name, true);
    }

    protected String getHost()
    {
        return (String)getTestConfig("host");
    }

    protected int getPort()
    {
        return (Integer)getTestConfig("port");
    }

    protected String getUser()
    {
        return (String)getTestConfig("user");
    }

    protected String getPassword()
    {
        return (String)getTestConfig("password");
    }

    protected String getDatabase()
    {
        return (String)getTestConfig("database");
    }

    protected void dropTable(String table) throws SQLException
    {
        String sql = String.format("DROP TABLE %s", table);
        executeSQL(sql, true);
    }

    protected List<List<Object>> select(String table) throws SQLException
    {
        try (Connection connection = connect()) {
            try (Statement statement = connection.createStatement()) {
                List<List<Object>> rows = new ArrayList<List<Object>>();
                String sql = String.format("SELECT * FROM %s", table);
                System.out.println(sql);
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    while (resultSet.next()) {
                        List<Object> row = new ArrayList<Object>();
                        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                            row.add(getValue(resultSet, i));
                        }
                        rows.add(row);
                    }
                }
                // cannot sort by CLOB, so sort by Java
                Collections.sort(rows, new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> o1, List<Object> o2) {
                        return o1.toString().compareTo(o2.toString());
                    }
                });
                return rows;
            }
        }

    }

    protected Object getValue(ResultSet resultSet, int index) throws SQLException
    {
        return resultSet.getObject(index);
    }

    protected void executeSQL(String sql) throws SQLException
    {
        executeSQL(sql, false);
    }

    protected void executeSQL(String sql, boolean ignoreError) throws SQLException
    {
        if (!enabled) {
            return;
        }

        try (Connection connection = connect()) {
            try {
                connection.setAutoCommit(true);

                try (Statement statement = connection.createStatement()) {
                    System.out.println(String.format("Execute SQL : \"%s\".", sql));
                    statement.execute(sql);
                }

            } catch (SQLException e) {
                if (!ignoreError) {
                    throw e;
                }
            }
        }
    }

    protected void test(String ymlPath) throws Exception
    {
        if (!enabled) {
            return;
        }

        tester.run(convertYml(ymlPath));
    }

    protected String convertYml(String ymlName) throws Exception
    {
        StringBuilder builder = new StringBuilder();
        Pattern pathPrefixPattern = Pattern.compile("^ *path(_prefix)?: '(.*)'$");
        for (String line : Files.readLines(convertPath(ymlName), Charset.forName("UTF8"))) {
            line = convertYmlLine(line);
            Matcher matcher = pathPrefixPattern.matcher(line);
            if (matcher.matches()) {
                int group = 2;
                builder.append(line.substring(0, matcher.start(group)));
                builder.append(convertPath(matcher.group(group)).getAbsolutePath());
                builder.append(line.substring(matcher.end(group)));
            } else {
                builder.append(line);
            }
            builder.append(System.lineSeparator());
        }
        return builder.toString();
    }

    protected String convertYmlLine(String line)
    {
        line = line.replaceAll("#host#", getHost());
        line = line.replaceAll("#port#", Integer.toString(getPort()));
        line = line.replaceAll("#database#", getDatabase());
        line = line.replaceAll("#user#", getUser());
        line = line.replaceAll("#password#", getPassword());
        return line;
    }

    protected File convertPath(String name) throws URISyntaxException
    {
        return new File(getClass().getResource(name).toURI());
    }

    protected List<String> read(String path) throws IOException
    {
        return Files.readLines(new File(path), Charset.forName("UTF8"));
    }

    protected abstract Connection connect() throws SQLException;

}
