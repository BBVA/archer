package com.bbva.ddd.domain.commands.read;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
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
public class CommandConsumerTest {

    @DisplayName("Create command consumer and message ok")
    @Test
    public void createCommandConsumer() {
        final ApplicationConfig configuration = new AppConfiguration().init();
        final List<String> topics = new ArrayList<>();
        final CommandConsumer commandConsumer = new CommandConsumer(1, topics, null, configuration);

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("create"));
        recordHeaders.add(CommandHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("euuid"));

        final CommandRecord commandRecord = commandConsumer.message("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", null, recordHeaders);

        Assertions.assertAll("EventConsumer",
                () -> Assertions.assertNotNull(commandConsumer),
                () -> Assertions.assertNotNull(commandRecord),
                () -> Assertions.assertEquals("create", commandRecord.name()),
                () -> Assertions.assertEquals("euuid", commandRecord.entityUuid()),
                () -> Assertions.assertEquals("key", commandRecord.uuid())
        );
    }

}
