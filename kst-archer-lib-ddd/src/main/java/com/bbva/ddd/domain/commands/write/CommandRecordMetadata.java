package com.bbva.ddd.domain.commands.write;

import com.bbva.common.producers.PRecordMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CommandRecordMetadata extends PRecordMetadata {

    private String commandId;
    private String entityId;

    public CommandRecordMetadata(final RecordMetadata recordMetadata, final String commandId, final String entityId) {
        super(recordMetadata);
        this.commandId = commandId;
        this.entityId = entityId;
    }

    // public String topic() {
    // return this.recordMetadata.topic();
    // }
    //
    // public int partition() {
    // return this.recordMetadata.partition();
    // }
    //
    // public long offset() {
    // return this.recordMetadata.offset();
    // }
    //
    // public long timestamp() {
    // return this.recordMetadata.timestamp();
    // }

    public String commandId() {
        return this.commandId;
    }

    public String entityId() {
        return this.entityId;
    }
}
