package com.bbva.common.producers.record;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Producer record metadata
 */
public class PRecordMetadata {

    protected RecordMetadata recordMetadata;

    /**
     * Constructor
     */
    public PRecordMetadata() {

    }

    /**
     * Constructor
     *
     * @param recordMetadata metadata
     */
    public PRecordMetadata(final RecordMetadata recordMetadata) {
        this.recordMetadata = recordMetadata;
    }

    /**
     * Get the topic name
     *
     * @return topic name
     */
    public String topic() {
        return recordMetadata.topic();
    }

    /**
     * Get the partition id
     *
     * @return partition id
     */
    public int partition() {
        return recordMetadata.partition();
    }

    /**
     * Get the record offset
     *
     * @return offset
     */
    public long offset() {
        return recordMetadata.offset();
    }

    /**
     * Get the record time
     *
     * @return timestamp
     */
    public long timestamp() {
        return recordMetadata.timestamp();
    }

}
