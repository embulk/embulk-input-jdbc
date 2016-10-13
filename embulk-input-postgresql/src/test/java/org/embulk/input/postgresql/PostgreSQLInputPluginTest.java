package org.embulk.input.postgresql;

import static java.util.Locale.ENGLISH;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.embulk.input.AbstractJdbcInputPluginTest;
import org.embulk.input.PostgreSQLInputPlugin;
import org.embulk.spi.InputPlugin;
import org.junit.Test;

public class PostgreSQLInputPluginTest extends AbstractJdbcInputPluginTest
{
    @Override
    protected void prepare() throws SQLException
    {
        tester.addPlugin(InputPlugin.class, "postgresql", PostgreSQLInputPlugin.class);

        try {
            // Create User and Database
            psql(String.format(ENGLISH, "DROP DATABASE IF EXISTS %s;", getDatabase()));
            psql(String.format(ENGLISH, "DROP USER IF EXISTS %s;", getUser()));
            psql(String.format(ENGLISH, "CREATE USER %s WITH SUPERUSER PASSWORD '%s';", getUser(), getPassword()));
            psql(String.format(ENGLISH, "CREATE DATABASE %s WITH OWNER %s;", getDatabase(), getUser()));
        } catch (IOException e) {
            System.err.println(e);
            System.err.println("Warning: cannot prepare a database for testing embulk-input-postgresql.");
            // 1. install postgresql.
            // 2. add bin directory to path.
            // 3. set environment variable PGPASSWORD or write pgpassword in tests.yml
            return;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        enabled = true;

        // Insert Data
        String sql = "";
        sql += "DROP TABLE IF EXISTS input_hstore;";
        sql += "CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;";
        sql += "CREATE TABLE input_hstore (c1 hstore);";
        sql += "INSERT INTO input_hstore (c1) VALUES('\"a\" => \"b\"');";
        executeSQL(sql);
    }

    @Test
    public void testHstoreAsString() throws Exception
    {
        if (enabled) {
            test("/yml/input_hstore.yml");
            assertEquals(Arrays.asList("c1", "\"\"\"a\"\"=>\"\"b\"\"\""),
                    read("postgresql-input000.00.csv"));
        }
    }

    @Test
    public void testHstoreAsJson() throws Exception
    {
        if (enabled) {
            test("/yml/input_hstore2.yml");
            assertEquals(Arrays.asList("c1", "\"{\"\"a\"\":\"\"b\"\"}\""),
                    read("postgresql-input000.00.csv"));
        }
    }

    private void psql(String sql) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("psql", "-w", "-c", sql);
        String pgPassword = (String)getTestConfig("pgpassword", false);
        if (!StringUtils.isEmpty(pgPassword)) {
            pb.environment().put("PGPASSWORD", pgPassword);
        }
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

    @Override
    protected Connection connect() throws SQLException
    {
        return DriverManager.getConnection(String.format(ENGLISH, "jdbc:postgresql://%s:%d/%s", getHost(), getPort(), getDatabase()),
                getUser(), getPassword());
    }
}
