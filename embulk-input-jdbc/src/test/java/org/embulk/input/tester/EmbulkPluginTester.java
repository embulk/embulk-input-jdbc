package org.embulk.input.tester;

import java.util.ArrayList;
import java.util.List;

import org.embulk.EmbulkEmbed;
import org.embulk.EmbulkEmbed.Bootstrap;
import org.embulk.config.ConfigSource;
import org.embulk.plugin.InjectedPluginSource;

import com.google.inject.Binder;
import com.google.inject.Module;

public class EmbulkPluginTester
{
    public static class PluginDefinition
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

    private EmbulkEmbed embulk;

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

    public List<PluginDefinition> getPlugins()
    {
        return plugins;
    }

    public void run(String yml) throws Exception
    {
        if (embulk == null) {
            Bootstrap bootstrap = new EmbulkEmbed.Bootstrap();
            bootstrap.addModules(new Module()
            {
                @Override
                public void configure(Binder binder)
                {
                    for (PluginDefinition plugin : plugins) {
                        InjectedPluginSource.registerPluginTo(binder, plugin.iface, plugin.name, plugin.impl);
                    }
                }
            });
            embulk = bootstrap.initializeCloseable();
        }
        ConfigSource config = embulk.newConfigLoader().fromYamlString(yml);
        embulk.run(config);
    }

    public void destroy() {
        if (embulk != null) {
            embulk.destroy();
            embulk = null;
        }
    }

}
