package com.bbva.common.producers;

import org.apache.kafka.clients.producer.RecordMetadata;

public class PRecordMetadata {

    protected RecordMetadata recordMetadata;

    public PRecordMetadata() {
    }

    public PRecordMetadata(final RecordMetadata recordMetadata) {
        this.recordMetadata = recordMetadata;
    }

    public String topic() {
        return this.recordMetadata.topic();
    }

    public int partition() {
        return this.recordMetadata.partition();
    }

    public long offset() {
        return this.recordMetadata.offset();
    }

    public long timestamp() {
        return this.recordMetadata.timestamp();
    }

}
