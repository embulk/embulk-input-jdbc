# SQL Server input plugins for Embulk

SQL Server input plugins for Embulk loads records from SQL Server.

## Overview

* **Plugin type**: input
* **Resume supported**: yes

## Configuration

- **driver_path**: path to the jar file of the SQL Server JDBC driver (string)
- **host**: database host name (string, required if url is not set)
- **port**: database port number (integer, default: 1433)
- **integratedSecutiry**: whether to use integrated authentication or not. The `sqljdbc_auth.dll` must be located on Java library path if using integrated authentication. : (boolean, default: false)
```
rem C:\drivers\sqljdbc_auth.dll
embulk "-J-Djava.library.path=C:\drivers" run input-sqlserver.yml
```
- **user**: database login user name (string, required if not using integrated authentication)
- **password**: database login password (string, default: "")
- **instance**: destination instance name (string, default: use the default instance)
- **database**: destination database name (string, default: use the default database)
- **url**: URL of the JDBC connection (string, optional)
- If you write SQL directly,
  - **query**: SQL to run (string)
- If **query** is not set,
  - **table**: destination table name (string, required)
  - **select**: comma-separated list of columns to select (string, default: "*")
  - **where**: WHERE condition to filter the rows (string, default: no-condition)
- **fetch_rows**: number of rows to fetch one time (used for java.sql.Statement#setFetchSize) (integer, default: 10000)
- **connect_timeout**: timeout for the driver to connect. 0 means the default of SQL Server (15 by default). (integer (seconds), default: 300)
- **socket_timeout**: timeout for executing the query. 0 means no timeout. (integer (seconds), default: 1800)
- **options**: extra JDBC properties (hash, default: {})
- **default_timezone**: If the sql type of a column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted int this default_timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **value_type**: embulk get values from database as this value_type. Typically, the value_type determines `getXXX` method of `java.sql.PreparedStatement`.
  (string, default: depends on the sql type of the column. Available values options are: `long`, `double`, `float`, `decimal`, `boolean`, `string`, `date`, `time`, `timestamp`)
  - **type**: Column values are converted to this embulk type.
  Available values options are: `boolean`, `long`, `double`, `string`, `timestamp`).
  By default, the embulk type is determined according to the sql type of the column (or value_type if specified).
  - **timestamp_format**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted by this timestamp_format. And if the embulk type is `timestamp`, this timestamp_format may be used in the output plugin. For example, stdout plugin use the timestamp_format, but *csv formatter plugin doesn't use*. (string, default : `%Y-%m-%d` for `date`, `%H:%M:%S` for `time`, `%Y-%m-%d %H:%M:%S` for `timestamp`)
  - **timezone**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted in this timezone.
(string, value of default_timezone option is used by default)

## Example

```yaml
in:
  type: sqlserver
  driver_path: C:\drivers\sqljdbc41.jar
  host: localhost
  user: myuser
  password: ""
  instance: MSSQLSERVER
  database: my_database
  table: my_table
  select: "col1, col2, col3"
  where: "col4 != 'a'"
```

If you need a complex SQL,

```yaml
in:
  type: sqlserver
  driver_path: C:\drivers\sqljdbc41.jar
  host: localhost
  user: myuser
  password: ""
  instance: MSSQLSERVER
  database: my_database
  query: |
    SELECT t1.id, t1.name, t2.id AS t2_id, t2.name AS t2_name
    FROM table1 AS t1
    LEFT JOIN table2 AS t2
      ON t1.id = t2.t1_id
```

Advanced configuration:

```yaml
in:
  type: sqlserver
  driver_path: C:\drivers\sqljdbc41.jar
  host: localhost
  user: myuser
  password: ""
  instance: MSSQLSERVER
  database: my_database
  table: "my_table"
  select: "col1, col2, col3"
  where: "col4 != 'a'"
  column_options:
    col1: {type: long}
    col3: {type: string, timestamp_format: "%Y/%m/%d", timezone: "+0900"}

```

## Build

```
$ ./gradlew gem
```
