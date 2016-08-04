# JDBC input plugins for Embulk

JDBC input plugins for Embulk loads records to databases using JDBC drivers.

## MySQL

See [embulk-input-mysql](embulk-input-mysql/).

## PostgreSQL

See [embulk-input-postgresql](embulk-input-postgresql/).

## Oracle

See [embulk-input-oracle](embulk-input-oracle/).

## Redshift

See [embulk-input-redshift](embulk-input-redshift/).

## SQL Server

See [embulk-input-sqlserver](embulk-input-sqlserver/).

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
- **fetch_rows**: number of rows to fetch one time (integer, default: 10000)
- **connect_timeout**: not supported.
- **socket_timeout**: timeout for executing the query. 0 means no timeout. (integer (seconds), default: 1800)
- **options**: extra JDBC properties (hash, default: {})
- If you write SQL directly,
  - **query**: SQL to run (string)
- If **query** is not set,
  - **table**: destination table name (string, required)
  - **select**: expression of select (e.g. `id, created_at`) (string, default: "*")
  - **where**: WHERE condition to filter the rows (string, default: no-condition)
  - **order_by**: expression of ORDER BY to sort rows (e.g. `created_at DESC, id ASC`) (string, default: not sorted)
- **default_timezone**: If the sql type of a column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted int this default_timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **default_column_options**: column_options for each JDBC type as default. Key is a JDBC type (e.g. 'DATE', 'BIGINT'). Value is same as column_options's value.
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **value_type**: embulk get values from database as this value_type. Typically, the value_type determines `getXXX` method of `java.sql.PreparedStatement`. `value_type: json` is an exception which uses `getString` and parses the result as a JSON string.
  (string, default: depends on the sql type of the column. Available values options are: `long`, `double`, `float`, `decimal`, `boolean`, `string`, `json`, `date`, `time`, `timestamp`)
  - **type**: Column values are converted to this embulk type.
  Available values options are: `boolean`, `long`, `double`, `string`, `json`, `timestamp`).
  By default, the embulk type is determined according to the sql type of the column (or value_type if specified).
  - **timestamp_format**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted by this timestamp_format. And if the embulk type is `timestamp`, this timestamp_format may be used in the output plugin. For example, stdout plugin use the timestamp_format, but *csv formatter plugin doesn't use*. (string, default : `%Y-%m-%d` for `date`, `%H:%M:%S` for `time`, `%Y-%m-%d %H:%M:%S` for `timestamp`)
  - **timezone**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted in this timezone.
(string, value of default_timezone option is used by default)
- **after_select**: if set, this SQL will be executed after the SELECT query in the same transaction.



### Example

```yaml
in:
  type: jdbc
  driver_path: /opt/oracle/ojdbc6.jar
  driver_class: oracle.jdbc.driver.OracleDriver
  url: jdbc:oracle:thin:@127.0.0.1:1521:mydb
  user: myuser
  password: "mypassword"
  table: "my_table"
  select: "col1, col2, col3"
  where: "col4 != 'a'"
  order_by: "col1 DESC"
```

This configuration will generate following SQL:

```
SELECT col1, col2, col3
FROM "my_table"
WHERE col4 != 'a'
ORDER BY col1 DESC
```

If you need a complex SQL,

```yaml
in:
  type: jdbc
  driver_path: /opt/oracle/ojdbc6.jar
  driver_class: oracle.jdbc.driver.OracleDriver
  url: jdbc:oracle:thin:@127.0.0.1:1521:mydb
  user: myuser
  password: "mypassword"
  query: |
    SELECT t1.id, t1.name, t2.id AS t2_id, t2.name AS t2_name
    FROM table1 AS t1
    LEFT JOIN table2 AS t2
      ON t1.id = t2.t1_id
```

Advanced configuration:

```yaml
in:
  type: jdbc
  driver_path: /opt/oracle/ojdbc6.jar
  driver_class: oracle.jdbc.driver.OracleDriver
  url: jdbc:oracle:thin:@127.0.0.1:1521:mydb
  user: myuser
  password: "mypassword"
  table: "my_table"
  select: "col1, col2, col3"
  where: "col4 != 'a'"
  default_column_options:
    DATE: { type: string, timestamp_format: "%Y/%m/%d", timezone: "+0900"}
    BIGINT: { type: string }
  column_options:
    col1: {type: long}
    col3: {type: string, timestamp_format: "%Y/%m/%d", timezone: "+0900"}
  after_select: "update my_table set col5 = '1' where col4 != 'a'"

```

### Build

```
$ ./gradlew gem
```
