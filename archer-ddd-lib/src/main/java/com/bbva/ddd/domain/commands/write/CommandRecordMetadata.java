package com.bbva.ddd.domain.commands.write;

import com.bbva.common.producers.record.PRecordMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Metadata for commands
 */
public class CommandRecordMetadata extends PRecordMetadata {

    private final String commandId;
    private final String entityId;

    /**
     * Constructor
     *
     * @param recordMetadata specific record metadata
     * @param commandId      command identifier
     * @param entityId       if of the entity object of the command
     */
    public CommandRecordMetadata(final RecordMetadata recordMetadata, final String commandId, final String entityId) {
        super(recordMetadata);
        this.commandId = commandId;
        this.entityId = entityId;
    }

    /**
     * Get the command id
     *
     * @return command id
     */
    public String commandId() {
        return commandId;
    }

    /**
     * Get entity id
     *
     * @return the id
     */
    public String entityId() {
        return entityId;
    }
}
