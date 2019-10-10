package com.bbva.common.producers;

import com.bbva.common.producers.record.PRecordMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

import java.util.Date;

@RunWith(JUnit5.class)
public class PRecordMetadataTest {

    @DisplayName("Create record metadata")
    @Test
    public void createCahedProducerOk() {

        final long timestamp = new Date().getTime();
        final RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition("topic", 1),
                1L, 1L, timestamp,
                1L, 9, 0);
        final PRecordMetadata pRecordMetadata = new PRecordMetadata(recordMetadata);
        final PRecordMetadata otherRecordMetadata = new PRecordMetadata();

        Assertions.assertAll("record metadata",
                () -> Assertions.assertEquals("topic", pRecordMetadata.topic()),
                () -> Assertions.assertEquals(2L, pRecordMetadata.offset()),
                () -> Assertions.assertEquals(1, pRecordMetadata.partition()),
                () -> Assertions.assertEquals(timestamp, pRecordMetadata.timestamp()),
                () -> Assertions.assertNotNull(otherRecordMetadata)
        );
    }

}
