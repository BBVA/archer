package com.bbva.ddd.domain.events.read;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.EventHeaderType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

public class EventRecord extends CRecord {

    public EventRecord(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType,
                       final String key, final SpecificRecord value, final RecordHeaders headers) {
        super(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    public String productorName() {
        return headers.find(EventHeaderType.PRODUCTOR_NAME_KEY).asString();
    }

    public String name() {
        return headers.find(EventHeaderType.NAME_KEY).asString();
    }

    public String referenceId() {
        return headers.find(EventHeaderType.REFERENCE_ID_KEY).asString();
    }

}
