package com.bbva.ddd.domain.commands.write;

import com.bbva.common.producers.PRecordMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CommandRecordMetadata extends PRecordMetadata {

    private final String commandId;
    private final String entityId;

    public CommandRecordMetadata(final RecordMetadata recordMetadata, final String commandId, final String entityId) {
        super(recordMetadata);
        this.commandId = commandId;
        this.entityId = entityId;
    }

    public String commandId() {
        return commandId;
    }

    public String entityId() {
        return entityId;
    }
}
