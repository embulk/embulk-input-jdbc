Embulk::JavaPlugin.register_input(
  :oracle, "org.embulk.input.OracleInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
