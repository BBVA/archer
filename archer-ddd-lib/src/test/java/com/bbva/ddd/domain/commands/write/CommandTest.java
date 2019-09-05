package com.bbva.ddd.domain.commands.write;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.headers.OptionalRecordHeaders;
import com.bbva.ddd.domain.HelperDomain;
import com.bbva.ddd.domain.callback.DefaultProducerCallback;
import com.bbva.ddd.domain.commands.write.records.PersonalData;
import com.bbva.ddd.domain.exceptions.ProduceException;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.ArrayList;
import java.util.concurrent.Future;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({CachedProducer.class, Command.class})
public class CommandTest {


    @DisplayName("Create command ok")
    @Test
    public void createCommandOk() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "add", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        HelperDomain.create(new ApplicationConfig());
        final Command command = new Command("topicBaseName", new ApplicationConfig(), false);
        final CommandRecordMetadata metadata = command.create(new PersonalData(), null, new DefaultProducerCallback());

        Assertions.assertAll("Command",
                () -> Assertions.assertNotNull(metadata),
                () -> Assertions.assertNotNull(metadata.commandId())
        );
    }

    @DisplayName("Process command action ok")
    @Test
    public void processsActionOk() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "add", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        HelperDomain.create(new ApplicationConfig());
        final Command command = new Command("topicBaseName", new ApplicationConfig(), false);
        final CommandRecordMetadata metadata = command.processAction("action", "entityId",
                new PersonalData(), new DefaultProducerCallback());

        Assertions.assertAll("Command",
                () -> Assertions.assertNotNull(metadata),
                () -> Assertions.assertNotNull(metadata.commandId()),
                () -> Assertions.assertNotNull("entityId", metadata.entityId())
        );
    }

    @DisplayName("Process delete command action ok")
    @Test
    public void deleteOk() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "remove", Mockito.any(), Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        HelperDomain.create(new ApplicationConfig());
        final Command command = new Command("topicBaseName", new ApplicationConfig(), false);
        final CommandRecordMetadata metadata = command.delete("entityId",
                PersonalData.class, new DefaultProducerCallback());

        Assertions.assertAll("Command",
                () -> Assertions.assertNotNull(metadata),
                () -> Assertions.assertNotNull(metadata.commandId()),
                () -> Assertions.assertNotNull("entityId", metadata.entityId())
        );
    }

    @DisplayName("Process delete command action ok")
    @Test
    public void deleteKo() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "remove", Mockito.any(), Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        HelperDomain.create(new ApplicationConfig());
        final Command command = new Command("topicBaseName", new ApplicationConfig(), false);

        Assertions.assertThrows(ApplicationException.class, () ->
                command.delete(null,
                        PersonalData.class, new DefaultProducerCallback())
        );

    }

    @DisplayName("Create event and send produce ProduceException ko")
    @Test
    public void createCommandKoProduceException() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.doThrow(new ProduceException()).when(producer, "add", Mockito.any(), Mockito.any());

        Assertions.assertThrows(ProduceException.class, () -> {
            HelperDomain.create(new ApplicationConfig());
            final Command command = new Command("topicBaseName", new ApplicationConfig(), false);
            command.create(new PersonalData(), null, new DefaultProducerCallback());
        });
    }

    @DisplayName("Create event and send produce ApplicationException ko")
    @Test
    public void createCommandKo() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);

        Assertions.assertThrows(ApplicationException.class, () -> {
            HelperDomain.create(new ApplicationConfig());
            final Command command = new Command("topicBaseName", new ApplicationConfig(), false);
            final OptionalRecordHeaders optionalHeaders = new OptionalRecordHeaders(new ArrayList<>());
            command.create(null, optionalHeaders, new DefaultProducerCallback());
        });
    }

}
