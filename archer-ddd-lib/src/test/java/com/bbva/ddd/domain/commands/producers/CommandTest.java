package com.bbva.ddd.domain.commands.producers;

import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.OptionalRecordHeaders;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.ddd.domain.callback.DefaultProducerCallback;
import com.bbva.ddd.domain.commands.consumers.CommandRecord;
import com.bbva.ddd.domain.commands.producers.records.PersonalData;
import com.bbva.ddd.domain.exceptions.ProduceException;
import org.apache.kafka.common.header.Header;
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({Command.class})
public class CommandTest {


    @DisplayName("Create command ok")
    @Test
    public void createCommandOk() throws Exception {
        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final List<Header> lstHeaders = new ArrayList<>();
        lstHeaders.add(new Header() {
            @Override
            public String key() {
                return "key";
            }

            @Override
            public byte[] value() {
                return new ByteArrayValue("value").getBytes();
            }
        });
        final OptionalRecordHeaders headers = new OptionalRecordHeaders(lstHeaders);
        final CommandRecordMetadata metadata = new Command.Builder(null, producer, false)
                .action(Command.CREATE_ACTION).to("topicBaseName")
                .value(new PersonalData()).headers(headers).build()
                .send(new DefaultProducerCallback());

        Assertions.assertAll("Command",
                () -> Assertions.assertNotNull(metadata),
                () -> Assertions.assertNotNull(metadata.commandId())
        );
    }

    @DisplayName("Create command ok")
    @Test
    public void createReplayCommandOk() throws Exception {
        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final List<Header> lstHeaders = new ArrayList<>();
        lstHeaders.add(new Header() {
            @Override
            public String key() {
                return "key";
            }

            @Override
            public byte[] value() {
                return new ByteArrayValue("value").getBytes();
            }
        });
        final OptionalRecordHeaders headers = new OptionalRecordHeaders(lstHeaders);
        final CommandRecordMetadata metadata = new Command.Builder(null, producer, true)
                .action(Command.CREATE_ACTION).to("topicBaseName")
                .value(new PersonalData()).headers(headers).build()
                .send(new DefaultProducerCallback());

        Assertions.assertAll("Command",
                () -> Assertions.assertNotNull(metadata),
                () -> Assertions.assertNotNull(metadata.commandId())
        );
    }

    @DisplayName("Create command without headers ok")
    @Test
    public void createCommandWithoutHeadersOk() throws Exception {
        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final CommandRecordMetadata metadata = new Command.Builder(null, producer, false)
                .action(Command.CREATE_ACTION).to("topicBaseName")
                .value(new PersonalData()).persistent().build()
                .send(new DefaultProducerCallback());

        Assertions.assertAll("Command",
                () -> Assertions.assertNotNull(metadata),
                () -> Assertions.assertNotNull(metadata.commandId())
        );
    }

    @DisplayName("Process command action ok")
    @Test
    public void processActionOk() throws Exception {
        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final Command command = new Command.Builder(null, producer, false)
                .action("action").to("topicBaseName")
                .uuid("entityId").value(new PersonalData())
                .build();
        final CommandRecordMetadata metadata = command.send(new DefaultProducerCallback());

        Assertions.assertAll("Command",
                () -> Assertions.assertNotNull(metadata),
                () -> Assertions.assertNotNull(metadata.commandId()),
                () -> Assertions.assertNotNull("entityId", metadata.entityId())
        );
    }

    @DisplayName("Process delete command action ok")
    @Test
    public void deleteOk() throws Exception {
        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final Command command = new Command.Builder(null, producer, false)
                .action(Command.DELETE_ACTION).to("topicBaseName")
                .uuid("entityId")
                .build();
        final CommandRecordMetadata metadata = command.send(new DefaultProducerCallback());

        Assertions.assertAll("Command",
                () -> Assertions.assertNotNull(metadata),
                () -> Assertions.assertNotNull(metadata.commandId()),
                () -> Assertions.assertNotNull("entityId", metadata.entityId())
        );
    }

    @DisplayName("Process delete command action ok")
    @Test
    public void deleteKo() throws Exception {
        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final Command command = new Command.Builder(null, producer, false)
                .action(Command.DELETE_ACTION).to("topicBaseName")
                .build();

        final CommandRecordMetadata metadata = command.send(new DefaultProducerCallback());

        Assertions.assertAll("Command",
                () -> Assertions.assertNotNull(metadata),
                () -> Assertions.assertNotNull(metadata.commandId()),
                () -> Assertions.assertNotNull("entityId", metadata.entityId())
        );
    }

    @DisplayName("Create event and send produce ProduceException ko")
    @Test
    public void createCommandKoProduceException() throws Exception {
        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.doThrow(new ProduceException()).when(producer, "send", Mockito.any(), Mockito.any());

        Assertions.assertThrows(ProduceException.class, () -> {
            final RecordHeaders headers = new RecordHeaders();
            headers.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("type-key"));

            final Command command = new Command.Builder(new CommandRecord("topic", 1, 1, new Date().getTime(), TimestampType.CREATE_TIME, "key", new PersonalData(), headers), producer, false)
                    .to("topicBaseName").value(new PersonalData()).action(Command.CREATE_ACTION)
                    .build();
            command.send(new DefaultProducerCallback());

        });
    }

}
