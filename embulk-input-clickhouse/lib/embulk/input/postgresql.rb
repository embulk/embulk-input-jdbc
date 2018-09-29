Embulk::JavaPlugin.register_input(
  :postgresql, "org.embulk.input.PostgreSQLInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
