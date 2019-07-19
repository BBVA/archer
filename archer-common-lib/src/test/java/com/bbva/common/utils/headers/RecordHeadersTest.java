package com.bbva.common.utils.headers;

import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.common.utils.headers.types.EventHeaderType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

@RunWith(JUnit5.class)
public class RecordHeadersTest {

    @DisplayName("Check add new header and to string method")
    @Test
    public void checkAddHeaderAndToString() {
        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("key", new ByteArrayValue("value"));

        Assertions.assertAll("headertypes",
                () -> Assertions.assertEquals("value", recordHeaders.find("key").asString()),
                () -> Assertions.assertEquals("[{key: key, value: value}]", recordHeaders.toString())
        );
    }

    @DisplayName("Check add with header types")
    @Test
    public void checkAddHeaderTypes() {
        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(ChangelogHeaderType.UUID_KEY, new ByteArrayValue("changelog-uuid"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("entity-uuid"));
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("type-key"));
        recordHeaders.add(EventHeaderType.PRODUCER_NAME_KEY, new ByteArrayValue("producer-name"));


        Assertions.assertAll("headertypes",
                () -> Assertions.assertEquals("changelog-uuid", recordHeaders.find(ChangelogHeaderType.UUID_KEY).asString()),
                () -> Assertions.assertEquals("entity-uuid", recordHeaders.find(CommandHeaderType.ENTITY_UUID_KEY).asString()),
                () -> Assertions.assertEquals("type-key", recordHeaders.find(CommonHeaderType.TYPE_KEY).asString()),
                () -> Assertions.assertEquals("producer-name", recordHeaders.find(EventHeaderType.PRODUCER_NAME_KEY).asString())
        );
    }

}
