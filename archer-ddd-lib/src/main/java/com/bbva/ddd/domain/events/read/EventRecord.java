package com.bbva.ddd.domain.events.read;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.EventHeaderType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * Wrap records of kind event and offer methods to parse event type headers.
 */
public class EventRecord extends CRecord {

    public EventRecord(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType,
                       final String key, final SpecificRecord value, final RecordHeaders headers) {
        super(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    /**
     * Get the producer name of the event record
     *
     * @return producer name of the event record
     */
    public String producerName() {
        return headers.find(EventHeaderType.PRODUCER_NAME_KEY).asString();
    }

    /**
     * Get the name of the event record
     *
     * @return name of the event record
     */
    public String name() {
        return headers.find(EventHeaderType.NAME_KEY).asString();
    }

    /**
     * Get the record that triggered it
     *
     * @return record which triggered this event
     */
    public CRecord referenceRecord() {
        return (CRecord) headers.find(EventHeaderType.REFERENCE_RECORD_KEY).as();
    }

}
