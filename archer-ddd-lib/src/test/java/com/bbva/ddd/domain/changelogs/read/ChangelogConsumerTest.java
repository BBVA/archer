package com.bbva.ddd.domain.commands.read;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import com.bbva.ddd.domain.changelogs.read.ChangelogConsumer;
import com.bbva.ddd.domain.changelogs.read.ChangelogRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;

@RunWith(JUnit5.class)
public class ChangelogConsumerTest {

    @DisplayName("Create changelog consumer and message ok")
    @Test
    public void createChangelogConsumer() throws Exception {
        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "add", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));


        final ApplicationConfig configuration = new AppConfiguration().init();
        final List<String> topics = new ArrayList<>();
        final ChangelogConsumer changelogConsumer = new ChangelogConsumer(1, topics, null, configuration);

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(ChangelogHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_UUID_KEY, new ByteArrayValue("euuid"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_NAME_KEY, new ByteArrayValue("aggName"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_METHOD_KEY, new ByteArrayValue("aggMethod"));

        final ChangelogRecord changelogRecord = changelogConsumer.message("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", null, recordHeaders);

        Assertions.assertAll("ChangelogConsumer",
                () -> Assertions.assertNotNull(changelogConsumer),
                () -> Assertions.assertNotNull(changelogRecord),
                () -> Assertions.assertEquals("key", changelogRecord.uuid()),
                () -> Assertions.assertEquals("euuid", changelogRecord.aggregateUuid()),
                () -> Assertions.assertEquals("aggName", changelogRecord.aggregateName()),
                () -> Assertions.assertEquals("aggMethod", changelogRecord.aggregateMethod())
        );
    }

}