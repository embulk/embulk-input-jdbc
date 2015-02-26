Embulk::JavaPlugin.register_input(
  :jdbc, "org.embulk.input.JdbcInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
