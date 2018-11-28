package com.bbva.ddd.domain.events.read;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.utils.RecordHeaders;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

public class EventRecord extends CRecord {

    public static final String PRODUCTOR_NAME_KEY = "productor.name";

    public EventRecord(String topic, int partition, long offset, long timestamp, TimestampType timestampType,
            String key, SpecificRecord value, RecordHeaders headers) {
        super(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    public String productorName() {
        return headers.find(EventRecord.PRODUCTOR_NAME_KEY).asString();
    }

}
