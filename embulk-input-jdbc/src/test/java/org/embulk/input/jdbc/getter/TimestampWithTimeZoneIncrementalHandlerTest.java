package org.embulk.input.jdbc.getter;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Timestamp;
import java.util.List;

import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigSource;
import org.embulk.exec.ExecModule;
import org.embulk.exec.SystemConfigModule;
import org.embulk.guice.Bootstrap;
import org.embulk.jruby.JRubyScriptingModule;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecAction;
import org.embulk.spi.ExecSession;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;

public class TimestampWithTimeZoneIncrementalHandlerTest extends TimestampIncrementalHadlerTestBase
{
    @Test
    public void test() throws Exception
    {
        ConfigSource systemConfig = EmbulkEmbed.newSystemConfigLoader().newConfigSource();
        List<Module> modules = ImmutableList.of(
                new SystemConfigModule(systemConfig),
                new ExecModule(),
                new JRubyScriptingModule(systemConfig));
        Bootstrap bootstrap = new Bootstrap()
                .requireExplicitBindings(false)
                .addModules(modules);
        ExecSession session = ExecSession.builder(bootstrap.initialize()).build();

        Exec.doWith(session, new ExecAction<Object>() {
            @Override
            public Object run() throws Exception {
                Timestamp value = createTimestamp("2016/01/23 12:34:56", 123456000);
                TimestampWithTimeZoneIncrementalHandler getter = new TimestampWithTimeZoneIncrementalHandler(new TimestampColumnGetter(createPageBuilder(), null, null));
                setTimestamp(getter, value);

                JsonNode json = getter.encodeToJson();
                assertThat(json.toString(), is("\"2016-01-23T10:34:56.123456Z\""));

                Timestamp result = decodeTimestamp(getter, json);
                assertThat(result, is(value));

                return null;
            }
        });

    }

}
