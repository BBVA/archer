package com.bbva.ddd.domain.commands.read;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * Wrap records of kind command and offer methods to parse command type headers.
 */
public class CommandRecord extends CRecord {

    public CommandRecord(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType,
                         final String key, final SpecificRecord value, final RecordHeaders headers) {
        super(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    /**
     * Get the uuid of the command record
     *
     * @return uuid of the command record
     */
    public String uuid() {
        return headers.find(CommandHeaderType.UUID_KEY).asString();
    }

    /**
     * Get the name of the command record
     *
     * @return name of the command record
     */
    public String name() {
        return headers.find(CommandHeaderType.NAME_KEY).asString();
    }

    /**
     * Get the entity identifier on which the action of the command applies
     *
     * @return entity identifier on which the action of the command applies
     */
    public String entityUuid() {
        return headers.find(CommandHeaderType.ENTITY_UUID_KEY).asString();
    }

}
