package org.embulk.input.postgresql;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.embulk.input.EmbulkPluginTester;
import org.embulk.input.PostgreSQLInputPlugin;
import org.embulk.spi.InputPlugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class PostgreSQLInputPluginTest
{
    private static final String DATABASE = "test_db";
    private static final String USER = "test_user";
    private static final String PASSWORD = "test_pw";
    private static final String URL = "jdbc:postgresql://localhost:5432/" + DATABASE;

    public static EmbulkPluginTester tester = new EmbulkPluginTester(InputPlugin.class, "postgresql", PostgreSQLInputPlugin.class);

    @BeforeClass
    public static void prepare() throws Exception
    {
        // Create User and Database
        psql(String.format("DROP DATABASE IF EXISTS %s;", DATABASE));
        psql(String.format("DROP USER IF EXISTS %s;", USER));
        psql(String.format("CREATE USER %s WITH SUPERUSER PASSWORD '%s';", USER, PASSWORD));
        psql(String.format("CREATE DATABASE %s WITH OWNER %s;", DATABASE, USER));

        // Insert Data
        try(Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            try (Statement statement = connection.createStatement()) {
                String sql = "";
                sql += "DROP TABLE IF EXISTS input_hstore;";
                sql += "CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;";
                sql += "CREATE TABLE input_hstore (c1 hstore);";
                sql += "INSERT INTO input_hstore (c1) VALUES('\"a\" => \"b\"');";
                statement.execute(sql);
            }
        }
    }

    @AfterClass
    public static void dispose()
    {
        tester.destroy();
    }

    @Test
    public void testHstoreAsString() throws Exception
    {
        tester.run(convertPath("/yml/input_hstore.yml"));
        assertEquals(Arrays.asList("c1", "\"\"\"a\"\"=>\"\"b\"\"\""),
                read("postgresql-input000.00.csv"));
    }

    @Test
    public void testHstoreAsJson() throws Exception
    {
        tester.run(convertPath("/yml/input_hstore2.yml"));
        assertEquals(Arrays.asList("c1", "\"{\"\"a\"\":\"\"b\"\"}\""),
                read("postgresql-input000.00.csv"));
    }

    private List<String> read(String path) throws IOException
    {
        FileSystem fs = FileSystems.getDefault();
        return Files.readAllLines(fs.getPath(path), Charset.defaultCharset());
    }

    private String convertPath(String name) throws URISyntaxException
    {
        if (getClass().getResource(name) == null) {
            return name;
        }
        return new File(getClass().getResource(name).toURI()).getAbsolutePath();
    }

    private static void psql(String sql) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("psql", "-w", "-c", sql);
        System.out.println("PSQL: " + pb.command().toString());
        final Process process = pb.start();
        final int code = process.waitFor();
        if (code != 0) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.err.println(line);
                }
            }
            throw new IOException(String.format(
                    "Command finished with non-zero exit code. Exit code is %d.", code));
        }
    }
}
