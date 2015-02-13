# PostgreSQL input plugins for Embulk

PostgreSQL input plugins for Embulk loads records from PostgreSQL.

## Overview

* **Plugin type**: input
* **Resume supported**: yes

## Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, 5432)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: destination name (string, default: "public")
- **table**: destination name (string, required)
- **select**: comma-separated list of columns to select (string, default: "*")
- **where**: WHERE condition to filter the rows (string, default: no-condition)
- **fetch_rows**: number of rows to fetch one time (used for java.sql.Statement#setFetchSize) (integer, default: 10000)
- **options**: extra JDBC properties (hash, default: {})

## Example

```yaml
in:
  type: postgresql
  host: localhost
  user: myuser
  password: ""
  database: my_database
  table: my_table
  select: "col1, col2, col3"
  where: "col4 != 'a'"
```

## Build

```
$ ./gradlew gem
```
