package com.bbva.ddd.domain.commands.read;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

public class CommandRecord extends CRecord {

    public CommandRecord(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType,
                         final String key, final SpecificRecord value, final RecordHeaders headers) {
        super(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    public String uuid() {
        return headers.find(CommandHeaderType.UUID_KEY).asString();
    }

    public String name() {
        return headers.find(CommandHeaderType.NAME_KEY).asString();
    }

    public String entityId() {
        return headers.find(CommandHeaderType.ENTITY_ID_KEY).asString();
    }

}
