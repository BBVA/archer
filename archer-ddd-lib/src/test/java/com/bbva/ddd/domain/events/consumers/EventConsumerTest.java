package com.bbva.ddd.domain.events.consumers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.EventHeaderType;
import com.bbva.ddd.domain.events.producers.Event;
import com.bbva.ddd.domain.handlers.HandlerContextImpl;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({Event.class, HandlerContextImpl.class})
public class EventConsumerTest {

    @DisplayName("Create event consumer and message ok")
    @Test
    public void createEventConsumer() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

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
