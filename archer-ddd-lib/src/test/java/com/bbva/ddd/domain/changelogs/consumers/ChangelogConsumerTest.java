package com.bbva.ddd.domain.changelogs.consumers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class ChangelogConsumerTest {

    @DisplayName("Create changelog consumer and message ok")
    @Test
    public void createChangelogConsumer() {

        final AppConfig configuration = ConfigBuilder.create();
        final List<String> topics = new ArrayList<>();
        final ChangelogConsumer changelogConsumer = new ChangelogConsumer(1, topics, null, configuration);

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(ChangelogHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_UUID_KEY, new ByteArrayValue("euuid"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_NAME_KEY, new ByteArrayValue("aggName"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_METHOD_KEY, new ByteArrayValue("aggMethod"));

        final ChangelogHandlerContext changelogHandlerContext = changelogConsumer.context(new CRecord("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", null, recordHeaders), PowerMockito.mock(DefaultProducer.class), false);

        Assertions.assertAll("ChangelogConsumer",
                () -> Assertions.assertNotNull(changelogConsumer),
                () -> Assertions.assertNotNull(changelogHandlerContext),
                () -> Assertions.assertEquals("key", changelogHandlerContext.consumedRecord().uuid()),
                () -> Assertions.assertEquals("euuid", changelogHandlerContext.consumedRecord().aggregateUuid()),
                () -> Assertions.assertEquals("aggName", changelogHandlerContext.consumedRecord().aggregateName()),
                () -> Assertions.assertEquals("aggMethod", changelogHandlerContext.consumedRecord().aggregateMethod()),
                () -> Assertions.assertEquals("topic", changelogHandlerContext.consumedRecord().source())
        );
    }

}
