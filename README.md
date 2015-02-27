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

- **driver_path**: path to the jar file of the JDBC driver (e.g. 'sqlite-jdbc-3.8.7.jar') (string, optional)
- **driver_class**: class name of the JDBC driver (e.g. 'org.sqlite.JDBC') (string, required)
- **url**: URL of the JDBC connection (e.g. 'jdbc:sqlite:mydb.sqlite3') (string, required)
- **user**: database login user name (string, optional)
- **password**: database login password (string, default: optional)
- **schema**: destination schema name (string, default: use the default schema)
- **table**: destination table name (string, required)
- **select**: comma-separated list of columns to select (string, default: "*")
- **where**: WHERE condition to filter the rows (string, default: no-condition)
- **fetch_rows**: number of rows to fetch one time (integer, default: 10000)
- **options**: extra JDBC properties (hash, default: {})

### Example

```yaml
in:
  type: jdbc
  driver_path: /opt/oracle/ojdbc6.jar
  driver_class: oracle.jdbc.driver.OracleDriver
  url: jdbc:oracle:thin:@myhost:1521:mydb
  user: myuser
  password: "mypassword"
  select: "col1, col2, col3"
  table: "my_table"
  where: "col4 != 'a'"
```

### Build

```
$ ./gradlew gem
```
