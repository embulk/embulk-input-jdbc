Embulk::JavaPlugin.register_input(
  :clickhouse, "org.embulk.input.ClickHouseInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
