Release 0.13.0 - 2022-07-05

* Update embulk-util-config to 0.3.1
* Use the MySQL/PostgreSQL JDBC driver found in the class loader in the highest priority
* Include the MySQL/PostgreSQL JDBC driver in pom.xml non-transitive dependencies so that the plugins can run in the Maven form
* Move packages of plugin classes from org.embulk.input

Release 0.12.4 - 2022-05-31

* Add adal4j library for sqlserver

Release 0.12.3 - 2021-08-25

* Add statement timeout for PostgreSQL and Redshift
* Remove embulk-input-db2 and embulk-input-oracle to be externalized in other repositories

Release 0.12.2 - 2021-06-16

* Fix Exec.newSomething() to CONFIG_MAPPER_FACTORY.newSomething()

Release 0.12.1 - 2021-05-31

* Release the JAR artifacts to Maven Central, instead of Bintray

Release 0.12.0 - 2021-05-31

* Catch up with Embulk API/SPI v0.10.
* Map an Embulk config directly to ZoneId with embulk-util-config's ZoneIdModule
* Wrap UnknownHostException to ConfigException

Release 0.11.1 - 2020-09-04

* Updated embulk-util-timestamp to 0.2.1 .
* Fixed a bug that incremental query with same length placeholders doesn't work.

Release 0.11.0 - 2020-07-15

* Updated the whole build process -- using the "org.embulk.embulk-plugins" Gradle plugin, and else.
* Caught up with the latest API/SPI -- getLogger, Optional, BufferAllocator, and else.
* Started to use embulk-util-timestamp in formatting and parsing timestamps.

Release 0.10.1 - 2019-09-06

* Changed the default JDBC driver for SQL Server from jTDS JDBC Driver to Microsoft JDBC Driver (@hieudion++).
* embulk-input-sqlserver supported socket_timeout for Microsoft JDBC Driver.

Release 0.10.0 - 2019-05-23

The following two are incompatible changes.
* Requires Java 8 or later (no longer supports Java 7), and requires embulk 0.9 or later.
* The default embulk type for MySQL JSON type was changed from `string` to `json` (@tvhung83++).

* Enabled to execute SQL before setting up and selecting by 'before_setup' option and 'before_select' option.
* Fixed missing left brace when getting empty array column in embulk-input-postgresql (@ommadawn46++).

Release 0.9.3 - 2018-08-10
* Added `use_raw_query_with_incremental` property in order to write optimized query for incremental loading.

Release 0.9.2 - 2018-07-03
* embulk-input-postgresql supported UUID type (@trung-huynh++).
* embulk-input-sqlserver supported datetime type column for incremental_columns option (@kmrshntr++).
* embulk-input-sqlserver added 'transaction_isolation_level' option (@kmrshntr++).

Release 0.9.1 - 2018-03-09

* Fixed the bug that embulk-input-postgresql might return null for array type (@puppycloud++).

Release 0.9.0 - 2017-12-27

* Detect oracle schema if not specified.
* Logging connection properties.
* Upgraded the bundled MySQL JDBC driver version from 5.1.34 to 5.1.44 (@hiroyuki-sato++, @y-ken++).
  Please check release notes for MySQL Connector/J (https://dev.mysql.com/doc/relnotes/connector-j/5.1/en/news-5-1.html) .
* embulk-input-mysql added `use_legacy_datetime_code` property.
  Its default value is `false` so that embulk-input-mysql will get correct datetime values when the server timezone and the client timezone are different.
  You can get the datetime values same as older embulk-output-mysql did by setting `true` to `use_legacy_datetime_code` (@hiroyuki-sato++).
* Fixed the bug that embulk-input-mysql incremental loading stored wrong timestamp values in configuration diff file
  when the server timezone isn't UTC, so couldn't load proper records at next execution (@ganchiku++).

Release 0.8.6 - 2017-11-24

* Supported PostgreSQL array type (@instcode++).
* Avoided using deprecated TimestampFormatter constructors.
* embulk-input-oracle detects schema if not specified so that it works correctly when another schema has the target table name.

Release 0.8.5 - 2017-07-31

* Enabled to change MySQL JDBC driver by 'driver_path' option.
* Enabled to change PostgreSQL JDBC driver by 'driver_path' option.
* Changed directory for JDBC drivers (Oracle, DB2) for the Gradle test task from 'driver' to 'test_jdbc_driver'.

Release 0.8.4 - 2017-06-23

* Added ssl option for embulk-input-mysql (@hiroyuki-sato++).
* Logging JDBC driver version (@hiroyuki-sato++).
* MySQL Connector/J version will be upgrade from 5.1.35 to 5.1.42 or higher in the near future.

Release 0.8.3 - 2017-05-26

* embulk-input-oracle incremental loading supported date and timestamp types (@kakoni++).
* embulk-input-redshift incremental loading supported timestamp and timestamptz types.
* embulk-input-mysql will warn if the client timezone is different from the server timezone
  because wrong datetime values will be fetched.
  `useLegacyDatetimeCode=false` will be set by default in future (@hiroyuki-sato++).

Release 0.8.2 - 2017-02-10

* Fixed the bug that prevents using non-standard SQL type even if value_type is set.

Release 0.8.1 - 2017-02-10

* Upgraded embulk to 0.8.15 .
* embulk-input-mysql incremental loading supported datetime and timestamp types.
* embulk-input-postgresql incremental loading supported timestamp and timestamptz types.
* Enabled to connect to Oracle with tnsnames.ora file (@pengfeiz++).
* Refactored tests to use embulk-test and Travis-CI.

Release 0.8.0 - 2016-10-13

* Added embulk-input-db2 to support IBM DB2.

Release 0.7.4 - 2016-09-28

* Corrected checking for table existence (@kasaharatt++).

Release 0.7.3 - 2016-08-26

* Supported SSL in Redshift (@yu-yamada++).
* Allowed DESC order at order_by option.
* Supported incremental load (@sakama++).
* embulk-input-sqlserver bundled jTDS, open source JDBC driver for SQL Server, and used it in default (@uu59++).
* Added application_name option in embulk-input-postgresql and embulk-input-sqlserver.

Release 0.7.2 - 2016-06-24

* Case insensitive for table names and column names.

Release 0.7.1 - 2016-05-07

* Running after_select after pageBuilder#finish because some output plugins do significant works in the method that may throw exceptions.

. committing output plugin.

Release 0.7.0 - 2016-03-29

* Supported json type (@joker1007++).
* Added 'default_column_options' to specify column options by SQL type. (@yoyama++)
* Supported hstore column in embulk-input-postgresql. (@kamatama41++)
* Added 'after_select' option to execute SQL after extracting records.

Release 0.6.4 - 2016-01-15

* Supported schema in SQL Server.
* Log query SQL before it is used for the first time, so that the SQL is logged even if it is invalid.

Release 0.6.3 - 2016-01-13

* Upgraded embulk version to 0.8.0.
  JSON type support will be added at a later major version.


Release 0.6.2 - 2016-01-04

* Supported schema in Oracle.


Release 0.6.1 - 2015-12-08

This release requires embulk >= 0.7.1.

* Added connect_timeout and socket_timeout to config.
* Supported SSL in PostrgreSQL (@kamatama41++).


Release 0.6.0 - 2015-07-06

* Upgraded embulk to v0.6.16
* Added support for SQL Server - embulk-input-sqlserver
* options: option accepts 'true', 'false' and integers without double quotes


Release 0.5.0 - 2015-06-17

* Upgraded embulk to v0.6.11
* Fixed a bug that causes embulk-input-mysql not to use column aliases in SQL 'as' clause (@torihat++)
* Enabled to specify embulk types and JDBC types by column_options
* Added embulk-input-oracle

Release 0.4.0 - 2015-03-05

* Added support for 'query' parameter.
* If 'query' parameter is set, 'table', 'select', 'where' and 'order_by' parameters are invalid.

Release 0.3.0 - 2015-02-27

* jdbc: url parameter replaced driver_name, host, port, and database
parameters. If you are using jdbc input plugin, you need to rewrite config
file as following:

Old configuration:

  driver_class: com.ibm.db2.jcc.DB2Driver
  driver_name: db2
  host: localhost
  port: 50000
  database: mydb

New configuration:

  driver_class: com.ibm.db2.jcc.DB2Driver
  url: jdbc:db2://localhost:50000/mydb

Release 0.2.4 - 2015-02-26

* Upgraded embulk to v0.4.10.
* jdbc: added driver_path parameter to load jar file dynamically.


Release 0.2.3 - 2015-02-25

* Upgraded embulk to v0.4.8
* Added guess method, which does nothing


Release 0.2.2 - 2015-02-17

* output/mysql: enhanced fetch_rows handling.
  If fetch_rows > 1, set useCursorFetch=true which enables server-side
  prepared statements. It fetches rows by chunks.
  If fetch_rows = 1, set Integer.MIN_VALUE to Statement.setFetchSize. It
  fetches rows one by one.
  If fetch_rows < 0, don't set anything. It fetches all rows at once
  to memory.


Release 0.2.1 - 2015-02-17

* Added support for DECIMAL type. DECIMAL is converted to double type
  (@shun0102++)
* output-mysql sets zeroDateTimeBehavior=convertToNull to the connection
  parameter to avoid 'SQLException: Value '0000-00-00' can not be represented
  as java.sql.Date' exception (@shun0102++)
