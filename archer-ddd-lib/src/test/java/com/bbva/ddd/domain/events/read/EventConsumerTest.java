package com.bbva.ddd.domain.events.read;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.EventHeaderType;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RunWith(JUnit5.class)
public class EventConsumerTest {

    @DisplayName("Create event consumer and message ok")
    @Test
    public void createEventConsumer() {
        final ApplicationConfig configuration = new AppConfiguration().init();
        final List<String> topics = new ArrayList<>();
        final EventConsumer eventConsumer = new EventConsumer(1, topics, null, configuration);

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(EventHeaderType.NAME_KEY, new ByteArrayValue("create"));
        recordHeaders.add(EventHeaderType.REFERENCE_RECORD_KEY, new ByteArrayValue("key"));
        recordHeaders.add(EventHeaderType.PRODUCER_NAME_KEY, new ByteArrayValue("producerName"));

        final EventRecord eventRecord = eventConsumer.message("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", null, recordHeaders);

        Assertions.assertAll("EventConsumer",
                () -> Assertions.assertNotNull(eventConsumer),
                () -> Assertions.assertNotNull(eventRecord),
                () -> Assertions.assertEquals("create", eventRecord.name()),
                () -> Assertions.assertEquals("producerName", eventRecord.producerName())
        );
    }

}
