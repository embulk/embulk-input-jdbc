Embulk::JavaPlugin.register_input(
  :cache, "org.embulk.input.CacheInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
