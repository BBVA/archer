package com.bbva.ddd.domain.events.read;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.record.CRecord;
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
        final AppConfig configuration = ConfigBuilder.create();
        final List<String> topics = new ArrayList<>();
        final EventConsumer eventConsumer = new EventConsumer(1, topics, null, configuration);

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(EventHeaderType.NAME_KEY, new ByteArrayValue("create"));
        recordHeaders.add(EventHeaderType.PRODUCER_NAME_KEY, new ByteArrayValue("producerName"));

        final EventHandlerContext eventHandlerContext = eventConsumer.context(new CRecord("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", null, recordHeaders));

        Assertions.assertAll("EventConsumer",
                () -> Assertions.assertNotNull(eventConsumer),
                () -> Assertions.assertNotNull(eventHandlerContext),
                () -> Assertions.assertEquals("create", eventHandlerContext.consumedRecord().name()),
                () -> Assertions.assertEquals("producerName", eventHandlerContext.consumedRecord().producerName())
        );
    }

}
