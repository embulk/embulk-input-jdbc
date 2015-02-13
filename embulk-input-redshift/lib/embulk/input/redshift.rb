Embulk::JavaPlugin.register_input(
  :redshift, "org.embulk.input.RedshiftInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
