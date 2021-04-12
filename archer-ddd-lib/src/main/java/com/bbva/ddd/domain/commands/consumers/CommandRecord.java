package com.bbva.ddd.domain.commands.consumers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.record.CRecord;
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

    public CommandRecord(final CRecord record) {
        super(record.topic(), record.partition(), record.offset(), record.timestamp(), record.timestampType(), record.key(), record.value(), record.recordHeaders());
    }

//    /**
//     * Get the uuid of the command record
//     *
//     * @return uuid of the command uuid
//     */
//    public String uuid() {
//        return headers.find(CommandHeaderType.KEY_KEY).asString();
//    }

    /**
     * Get the name of the command record
     *
     * @return name of the action
     */
    public String action() {
        return headers.find(CommandHeaderType.ACTION_KEY).asString();
    }

    /**
     * Get the entity identifier on which the action of the command applies
     *
     * @return entity identifier on which the action of the command applies
     */
    public String entityUuid() {
        return headers.find(CommandHeaderType.ENTITY_UUID_KEY).asString();
    }

    /**
     * Get the source of the command
     *
     * @return command source
     */
    public String source() {
        return topic().replace(AppConfig.COMMANDS_RECORD_NAME_SUFFIX, "");
    }
}