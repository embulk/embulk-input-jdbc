# Redshift input plugins for Embulk

Redshift input plugins for Embulk loads records from Redshift.

## Overview

* **Plugin type**: input
* **Resume supported**: yes

## Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, 5439)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: destination schema name (string, default: "public")
- **where**: WHERE condition to filter the rows (string, default: no-condition)
- **fetch_rows**: number of rows to fetch one time (used for java.sql.Statement#setFetchSize) (integer, default: 10000)
- **options**: extra JDBC properties (hash, default: {})
- If you write SQL directly,
  - **query**: SQL to run (string)
- If **query** is not set,
  - **table**: destination table name (string, required)
  - **select**: comma-separated list of columns to select (string, default: "*")
  - **where**: WHERE condition to filter the rows (string, default: no-condition)
- **default_timezone**: If the sql type of a column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted int this default_timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: Column values are converted to this embulk type.
  Available values options are: `boolean`, `long`, `double`, `string`, `timestamp`).
  By default, the embulk type is determined according to the sql type of the column.
  - **timestamp_format**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted by this timestamp_format. And if the embulk type is `timestamp`, this timestamp_format will be used in the output plugin. (string, default : `%Y-%m-%d` for `date`, `%H:%M:%S` for `time`, `%Y-%m-%d %H:%M:%S` for `timestamp`)
  - **timezone**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted in this timezone.
(string, value of default_timezone option is used by default)

## Example

```yaml
in:
  type: redshift
  host: localhost
  user: myuser
  password: ""
  database: my_database
  table: my_table
  select: "col1, col2, col3"
  where: "col4 != 'a'"
```

If you need a complex SQL,

```yaml
in:
  type: redshift
  host: localhost
  user: myuser
  password: ""
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
  type: redshift
  host: localhost
  user: myuser
  password: ""
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
