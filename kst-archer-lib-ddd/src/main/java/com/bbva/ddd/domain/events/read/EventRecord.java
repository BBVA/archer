package com.bbva.ddd.domain.events.read;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.utils.RecordHeaders;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

public class EventRecord extends CRecord {

    public static final String PRODUCTOR_NAME_KEY = "productor.name";
    public static final String NAME_KEY = "name";
    public static final String REFERENCE_ID = "reference.id";

    public EventRecord(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType,
                       final String key, final SpecificRecord value, final RecordHeaders headers) {
        super(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    public String productorName() {
        return headers.find(EventRecord.PRODUCTOR_NAME_KEY).asString();
    }

    public String name() {
        return headers.find(EventRecord.NAME_KEY).asString();
    }

    public String referenceId() {
        return headers.find(EventRecord.REFERENCE_ID).asString();
    }

}
