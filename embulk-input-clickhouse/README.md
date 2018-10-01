# ClickHouse input plugin for Embulk

ClickHouse input plugin for Embulk loads records from ClickHouse.

## Overview

* **Plugin type**: input
* **Resume supported**: **NO**

## Configuration

- **driver_path**: path to the jar file of the ClickHouse JDBC driver. If not set, the bundled JDBC driver (ClickHouse JDBC Driver **TBD**) will be used. (string)
- **host**: database host name (string, required)
- **port**: database port number (integer, 8123)
- **user**: database login user name (string)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **buffer_size**: see ClickHouse param **buffer_size** (integer, default: 65535)
- **apache_buffer_size**: see ClickHouse param **apache_buffer_size** (integer, default: 65535)
- **connect_timeout**: see ClickHouse param **connection_timeout** (integer, default: 30000)
- **socket_timeout**: see ClickHouse param **socket_timeout** (integer, default: 10000)
- **data_transfer_timeout**: see ClickHouse param **data_transfer_timeout** (integer, default: 10000)
- **keep_alive_timeout**: see ClickHouse param **keep_alive_timeout** (integer, default: 10000)
- **options**: extra JDBC properties (hash, default: {})
- If you write SQL directly,
  - **query**: SQL to run (string)
  - **use_raw_query_with_incremental**: If true, you can write optimized query using prepared statement by yourself. See [Use incremental loading with raw query](#use-incremental-loading-with-raw-query) for more detail (boolean, default: false)
- If **query** is not set,
  - **table**: destination table name (string, required)
  - **select**: expression of select (e.g. `id, created_at`) (string, default: "*")
  - **where**: WHERE condition to filter the rows (string, default: no-condition)
  - **order_by**: expression of ORDER BY to sort rows (e.g. `created_at DESC, id ASC`) (string, default: not sorted)
- **default_timezone**: If the sql type of a column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted int this default_timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **value_type**: embulk get values from database as this value_type. Typically, the value_type determines `getXXX` method of `java.sql.PreparedStatement`.
  (string, default: depends on the sql type of the column. Available values options are: `long`, `double`, `float`, `decimal`, `boolean`, `string`, `json`, `date`, `time`, `timestamp`, `array`)
  See below for `hstore` column.
  - **type**: Column values are converted to this embulk type.
  Available values options are: `boolean`, `long`, `double`, `string`, `json`, `timestamp`).
  By default, the embulk type is determined according to the sql type of the column (or value_type if specified).
  In default, 'UInt64' values are converted to `long`, but too large values can't be converted to `long`.
  So, please use `string` or `json`
  - **timestamp_format**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted by this timestamp_format. And if the embulk type is `timestamp`, this timestamp_format may be used in the output plugin. For example, stdout plugin use the timestamp_format, but *csv formatter plugin doesn't use*. (string, default : `%Y-%m-%d` for `date`, `%H:%M:%S` for `time`, `%Y-%m-%d %H:%M:%S` for `timestamp`)
  - **timezone**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted in this timezone.
(string, value of default_timezone option is used by default)
- **after_select**: if set, this SQL will be executed after the SELECT query in the same transaction.

## Example

```yaml
in:
  type: clickhouse
  host: localhost
  database: my_database
  table: my_table
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

Advanced configuration:

```yaml
in:
  type: clickhouse
  driver_path: ./path/to/clickhouse-jdbc-x.y.jar
  database: my_database
  host: localhost
  socket_timeout:  1000000
  query: SELECT * from my_table
  column_options:
    col1: {type: string} # If col1 include too large integer value(UInt64), convert to string.
```

## Build

```
$ ./gradlew gem
```
