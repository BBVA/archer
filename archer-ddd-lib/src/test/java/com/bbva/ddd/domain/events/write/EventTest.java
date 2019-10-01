package com.bbva.ddd.domain.events.write;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.ddd.domain.HelperDomain;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.domain.commands.write.records.PersonalData;
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
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "add", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        HelperDomain.create(new ApplicationConfig());
        final Event event = new Event("topicBaseName", new ApplicationConfig());
        final EventRecordMetadata metadata = event.send("producerName", new PersonalData(), null);

        Assertions.assertAll("Command",
                () -> Assertions.assertNotNull(metadata),
                () -> Assertions.assertNotNull(metadata.transactionId())
        );
    }

    @DisplayName("Create event and send produce ProduceException ok")
    @Test
    public void createEventKo() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.doThrow(new ProduceException()).when(producer, "add", Mockito.any(), Mockito.any());

        Assertions.assertThrows(ProduceException.class, () -> {
            HelperDomain.create(new ApplicationConfig());
            final Event event = new Event("topicBaseName", new ApplicationConfig());
            event.send("producerName", null, null);
        });
    }

    @DisplayName("Create event and send produce ProduceException ok")
    @Test
    public void createEventKeyKo() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.doThrow(new ProduceException()).when(producer, "add", Mockito.any(), Mockito.any());

        Assertions.assertThrows(ProduceException.class, () -> {
            HelperDomain.create(new ApplicationConfig());
            final Event event = new Event("topicBaseName", new ApplicationConfig());
            event.send("key", "producerName", null, null);
        });
    }

    @DisplayName("Create event and send produce ProduceException ok")
    @Test
    public void createEvent2Ko() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.doThrow(new ProduceException()).when(producer, "add", Mockito.any(), Mockito.any());

        Assertions.assertThrows(ProduceException.class, () -> {
            HelperDomain.create(new ApplicationConfig());
            final Event event = new Event("topicBaseName", new ApplicationConfig());
            event.send("producerName", null, "name", null);
        });
    }

    @DisplayName("Create event and send produce ProduceException ok")
    @Test
    public void createEvent3Ko() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.doThrow(new ProduceException()).when(producer, "add", Mockito.any(), Mockito.any());

        Assertions.assertThrows(ProduceException.class, () -> {
            HelperDomain.create(new ApplicationConfig());
            final Event event = new Event("topicBaseName", new ApplicationConfig());
            event.send("producerName", null, true, new CommandRecord("topic", 1, 1, new Date().getTime(),
                    TimestampType.CREATE_TIME, "key", new PersonalData(), null), null);
        });
    }

}
