package com.bbva.ddd.domain.commands.read;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.utils.RecordHeaders;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

public class CommandRecord extends CRecord {

    public static final String UUID_KEY = "uuid";
    public static final String NAME_KEY = "name";
    public static final String ENTITY_ID_KEY = "entity.id";

    public CommandRecord(String topic, int partition, long offset, long timestamp, TimestampType timestampType,
            String key, SpecificRecord value, RecordHeaders headers) {
        super(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    public String uuid() {
        return headers.find(CommandRecord.UUID_KEY).asString();
    }

    public String name() {
        return headers.find(CommandRecord.NAME_KEY).asString();
    }

    public String entityId() {
        return headers.find(CommandRecord.ENTITY_ID_KEY).asString();
    }

}
