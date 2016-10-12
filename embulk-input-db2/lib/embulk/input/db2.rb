Embulk::JavaPlugin.register_input(
  :db2, "org.embulk.input.DB2InputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
