package com.bbva.ddd.domain.events.write;

import com.bbva.common.producers.PRecordMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;

public class EventRecordMetadata extends PRecordMetadata {

    private final String transactionId;

    public EventRecordMetadata(final RecordMetadata recordMetadata, final String transactionId) {
        super(recordMetadata);
        this.transactionId = transactionId;
    }

    public String transactionId() {
        return transactionId;
    }

}
