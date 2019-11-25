package com.bbva.ddd.domain.changelogs.consumers;

import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

@RunWith(JUnit5.class)
public class ChangelogRecordTest {

    @DisplayName("Create changelog ok")
    @Test
    public void createChangelogOk() {
        final ChangelogRecord changelogRecord = new ChangelogRecord("topic", 1,
                1, 1L, TimestampType.NO_TIMESTAMP_TYPE, "key", null, null);
        Assertions.assertAll("ChangelogRecord",
                () -> Assertions.assertNotNull(changelogRecord),
                () -> Assertions.assertNotNull(changelogRecord.source())
        );
    }

    @DisplayName("Create changelog with headers ok")
    @Test
    public void createChangelogHeadersOk() {

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommandHeaderType.ACTION_KEY, new ByteArrayValue("create"));
        recordHeaders.add(CommandHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_UUID_KEY, new ByteArrayValue("euid"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_NAME_KEY, new ByteArrayValue("agg-name"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_METHOD_KEY, new ByteArrayValue("method"));

        final ChangelogRecord changelogRecord = new ChangelogRecord("topic", 1,
                1, 1L, TimestampType.NO_TIMESTAMP_TYPE, "key", null, recordHeaders);


        Assertions.assertAll("ChangelogRecord",
                () -> Assertions.assertNotNull(changelogRecord),
                () -> Assertions.assertEquals("key", changelogRecord.uuid()),
                () -> Assertions.assertEquals("euid", changelogRecord.aggregateUuid()),
                () -> Assertions.assertEquals("agg-name", changelogRecord.aggregateName()),
                () -> Assertions.assertEquals("method", changelogRecord.aggregateMethod())
        );
    }

}
