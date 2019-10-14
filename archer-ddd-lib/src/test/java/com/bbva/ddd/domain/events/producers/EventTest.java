package com.bbva.ddd.domain.events.producers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.ddd.domain.HelperDomain;
import com.bbva.ddd.domain.commands.consumers.CommandRecord;
import com.bbva.ddd.domain.commands.producers.records.PersonalData;
import com.bbva.ddd.domain.exceptions.ProduceException;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.Date;
import java.util.concurrent.Future;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({CachedProducer.class, Event.class})
public class EventTest {

    @DisplayName("Create event and send produce ok")
    @Test
    public void createEventOk() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.when(producer, "add", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        HelperDomain.create(new AppConfig());

        final Event event = new Event.Builder(producer, null)
                .to("topicBaseName").producerName("producerName").value(new PersonalData())
                .build();
        final EventRecordMetadata metadata = event.send(null);

        Assertions.assertAll("Command",
                () -> Assertions.assertNotNull(metadata),
                () -> Assertions.assertNotNull(metadata.transactionId())
        );
    }

    @DisplayName("Create event and send produce ProduceException ok")
    @Test
    public void createEventKo() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.doThrow(new ProduceException()).when(producer, "add", Mockito.any(), Mockito.any());

        Assertions.assertThrows(ProduceException.class, () -> {
            HelperDomain.create(new AppConfig());

            final Event event = new Event.Builder(producer, null)
                    .to("topicBaseName").producerName("producerName")
                    .build();
            event.send(null);
        });
    }

    @DisplayName("Create event and send produce ProduceException ok")
    @Test
    public void createEventKeyKo() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.doThrow(new ProduceException()).when(producer, "add", Mockito.any(), Mockito.any());

        Assertions.assertThrows(ProduceException.class, () -> {
            HelperDomain.create(new AppConfig());
            final Event event = new Event.Builder(producer, null)
                    .to("topicBaseName").producerName("producerName").key("key")
                    .build();
            event.send(null);
        });
    }

    @DisplayName("Create event and send produce ProduceException ok")
    @Test
    public void createEvent2Ko() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.doThrow(new ProduceException()).when(producer, "add", Mockito.any(), Mockito.any());

        Assertions.assertThrows(ProduceException.class, () -> {
            HelperDomain.create(new AppConfig());
            final Event event = new Event.Builder(producer, null)
                    .to("topicBaseName").producerName("producerName").name("name")
                    .build();
            event.send(null);
        });
    }

    @DisplayName("Create event and send produce ProduceException ok")
    @Test
    public void createEvent3Ko() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.doThrow(new ProduceException()).when(producer, "add", Mockito.any(), Mockito.any());

        Assertions.assertThrows(ProduceException.class, () -> {
            HelperDomain.create(new AppConfig());
            final RecordHeaders headers = new RecordHeaders();
            headers.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("type-key"));

            final Event event = new Event.Builder(producer, new CommandRecord("topic", 1, 1, new Date().getTime(),
                    TimestampType.CREATE_TIME, "key", new PersonalData(), headers))
                    .to("topicBaseName").producerName("producerName").name("name")
                    .replay()
                    .build();
            event.send(null);
        });
    }

}
