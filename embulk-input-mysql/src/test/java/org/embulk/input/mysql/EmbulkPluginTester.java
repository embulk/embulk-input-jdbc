package org.embulk.input.mysql;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.embulk.EmbulkService;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.exec.BulkLoader;
import org.embulk.exec.ExecutionResult;
import org.embulk.plugin.InjectedPluginSource;
import org.embulk.spi.ExecSession;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;

public class EmbulkPluginTester
{
    private static class PluginDefinition
    {
        public final Class<?> iface;
        public final String name;
        public final Class<?> impl;


        public PluginDefinition(Class<?> iface, String name, Class<?> impl)
        {
            this.iface = iface;
            this.name = name;
            this.impl = impl;
        }

    }

    private final List<PluginDefinition> plugins = new ArrayList<PluginDefinition>();



    public EmbulkPluginTester()
    {
    }

    public EmbulkPluginTester(Class<?> iface, String name, Class<?> impl)
    {
        addPlugin(iface, name, impl);
    }

    public void addPlugin(Class<?> iface, String name, Class<?> impl)
    {
        plugins.add(new PluginDefinition(iface, name, impl));
    }

    public void run(String ymlPath) throws Exception
    {
        EmbulkService service = new EmbulkService(new EmptyConfigSource()) {
            @Override
            protected Iterable<? extends Module> getAdditionalModules(ConfigSource systemConfig)
            {
                return Arrays.asList(new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        for (PluginDefinition plugin : plugins) {
                            InjectedPluginSource.registerPluginTo(binder, plugin.iface, plugin.name, plugin.impl);
                        }
                    }
                });
            }
        };
        Injector injector = service.getInjector();
        ConfigSource config = injector.getInstance(ConfigLoader.class).fromYamlFile(new File(ymlPath));
        ExecSession session = new ExecSession(injector, config);
        BulkLoader loader = injector.getInstance(BulkLoader.class);
        ExecutionResult result = loader.run(session, config);
    }

}
