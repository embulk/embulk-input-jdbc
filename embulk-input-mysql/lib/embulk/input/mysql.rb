Embulk::JavaPlugin.register_input(
  :mysql, "org.embulk.input.MySQLInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
