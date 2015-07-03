Embulk::JavaPlugin.register_input(
  :sqlserver, "org.embulk.input.SQLServerInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
