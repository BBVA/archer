package com.bbva.ddd.domain.changelogs.producers;

import com.bbva.common.producers.record.PRecordMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Metadata for changelog records
 */
public class ChangelogRecordMetadata extends PRecordMetadata {

    /**
     * Constructor
     *
     * @param recordMetadata record metadata to create
     */
    public ChangelogRecordMetadata(final RecordMetadata recordMetadata) {
        super(recordMetadata);
    }

}
