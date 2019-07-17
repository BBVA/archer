package com.bbva.ddd.domain.changelogs.write;

import com.bbva.common.producers.PRecordMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ChangelogRecordMetadata extends PRecordMetadata {

    public ChangelogRecordMetadata(final RecordMetadata recordMetadata) {
        super(recordMetadata);
    }

}
