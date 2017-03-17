package org.embulk.input.jdbc.getter;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Timestamp;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

public class TimestampWithoutTimeZoneIncrementalHandlerTest extends TimestampIncrementalHadlerTestBase
{

    @Test
    public void test() throws Exception
    {
        Timestamp value = createTimestamp("2016/01/23 12:34:56", 123456000);
        TimestampWithoutTimeZoneIncrementalHandler getter = new TimestampWithoutTimeZoneIncrementalHandler(new TimestampColumnGetter(null, null, null));
        setTimestamp(getter, value);

        JsonNode json = getter.encodeToJson();
        assertThat(json.toString(), is("\"2016-01-23T12:34:56.123456\""));

        Timestamp result = decodeTimestamp(getter, json);
        assertThat(result, is(value));
    }

}
