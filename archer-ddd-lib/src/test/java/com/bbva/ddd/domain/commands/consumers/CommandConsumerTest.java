package com.bbva.ddd.domain.commands.consumers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.ddd.domain.changelogs.repository.RepositoryImpl;
import com.bbva.ddd.domain.commands.producers.Command;
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
@PrepareForTest({Command.class, RepositoryImpl.class})
public class CommandConsumerTest {

    @DisplayName("Create command consumer and message ok")
    @Test
    public void createCommandConsumer() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        final AppConfig configuration = ConfigBuilder.create();
        final List<String> topics = new ArrayList<>();
        final CommandConsumer commandConsumer = new CommandConsumer(1, topics, null, configuration);

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommandHeaderType.ACTION_KEY, new ByteArrayValue("create"));
        recordHeaders.add(CommandHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("euid"));

        final CommandHandlerContext commandHandlerContext = commandConsumer.context(new CRecord("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", null, recordHeaders), null, false);

        Assertions.assertAll("EventConsumer",
                () -> Assertions.assertNotNull(commandConsumer),
                () -> Assertions.assertNotNull(commandHandlerContext),
                () -> Assertions.assertEquals("create", commandHandlerContext.consumedRecord().action()),
                () -> Assertions.assertEquals("euid", commandHandlerContext.consumedRecord().entityUuid()),
                () -> Assertions.assertEquals("key", commandHandlerContext.consumedRecord().uuid())
        );
    }

}
