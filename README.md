# JDBC input plugins for Embulk

JDBC input plugins for Embulk loads records to databases using JDBC drivers.

**[WARNING!]**

- embulk-input-jdbc 0.10 requires Java 8 or later (no longer supports Java 7), and requires embulk 0.9 or later.
- The default embulk type for MySQL JSON type was changed from `string` to `json` since embulk-input-jdbc 0.10 .

## MySQL

See [embulk-input-mysql](embulk-input-mysql/).

## PostgreSQL

See [embulk-input-postgresql](embulk-input-postgresql/).

## Redshift

See [embulk-input-redshift](embulk-input-redshift/).

## SQL Server

See [embulk-input-sqlserver](embulk-input-sqlserver/).

## Others (generic JDBC)

See [embulk-input-jdbc](embulk-input-jdbc/).

## Installation via JitPack

You can install these plugins directly from GitHub using JitPack:

### Install from latest commit
```bash
# For PostgreSQL
gem install embulk-input-postgresql --source https://jitpack.io/

# For MySQL  
gem install embulk-input-mysql --source https://jitpack.io/

# For SQL Server
gem install embulk-input-sqlserver --source https://jitpack.io/

# For Redshift
gem install embulk-input-redshift --source https://jitpack.io/
```

### Install from specific branch or tag
```bash
# Install from a specific branch (e.g., fix/jackson-bind-cve)
gem install embulk-input-postgresql --source https://jitpack.io/com/github/embulk/embulk-input-jdbc/fix~jackson-bind-cve/

# Install from a specific tag (e.g., v0.14.0)  
gem install embulk-input-postgresql --source https://jitpack.io/com/github/embulk/embulk-input-jdbc/v0.14.0/
```

**Note**: Replace the plugin name (`embulk-input-postgresql`) with the specific plugin you want to install.
