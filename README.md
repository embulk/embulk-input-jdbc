# JDBC input plugins for Embulk

JDBC input plugins for Embulk loads records to databases using JDBC drivers.

## MySQL

See [embulk-input-mysql](embulk-input-mysql/).

## PostgreSQL

See [embulk-input-postgresql](embulk-input-postgresql/).

## Redshift

See [embulk-input-redshift](embulk-input-redshift/).

## Generic

### Overview

* **Plugin type**: input
* **Resume supported**: yes

### Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, required)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: destination name (string, default: use default schema)
- **table**: destination name (string, required)
- **select**: comma-separated list of columns to select (string, default: "*")
- **where**: WHERE condition to filter the rows (string, default: no-condition)
- **fetch_rows**: number of rows to fetch one time (integer, default: 10000)
- **options**: extra JDBC properties (hash, default: {})
- **driver_name**: name of the JDBC driver used in connection url (e.g. 'sqlite') (string, required)
- **driver_class**: class name of the JDBC driver (e.g. 'org.sqlite.JDBC') (string, required)
- **driver_path**: path to the jar file of the JDBC driver (e.g. 'sqlite-jdbc-3.8.7.jar') (string, optional)

### Example

In addtion to the configuration, you need to supply -C option to embulk command to add jar files to the classpath.

```yaml
in:
  type: jdbc
  host: localhost
  port: 1521
  user: myuser
  password: ""
  database: my_database
  table: my_table
  select: "col1, col2, col3"
  where: "col4 != 'a'"
  driver_name: oracle
  driver_class: oracle.jdbc.driver.OracleDriver
  driver_path: /opt/oracle/ojdbc6.jar
```

### Build

```
$ ./gradlew gem
```
