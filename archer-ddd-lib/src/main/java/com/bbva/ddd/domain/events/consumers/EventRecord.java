package com.bbva.ddd.domain.events.consumers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.EventHeaderType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * Wrap records of kind event and offer methods to parse event type headers.
 */
public class EventRecord extends CRecord {

    /**
     * Constructor
     *
     * @param topic         topic name
     * @param partition     partition in which the record is stored
     * @param offset        offset to find the element
     * @param timestamp     time
     * @param timestampType time type
     * @param key           record key
     * @param value         record data
     * @param headers       headers
     */
    public EventRecord(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType,
                       final String key, final SpecificRecord value, final RecordHeaders headers) {
        super(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    public EventRecord(final CRecord record) {
        super(record.topic(), record.partition(), record.offset(), record.timestamp(), record.timestampType(), record.key(), record.value(), record.recordHeaders());
    }

    /**
     * Get the producer name of the event record
     *
     * @return producer name of the event record
     */
    public String producerName() {
        return headers.find(EventHeaderType.PRODUCER_NAME_KEY.getName()).asString();
    }

    /**
     * Get the name of the event record
     *
     * @return name of the event record
     */
    public String name() {
        return headers.find(EventHeaderType.NAME_KEY.getName()).asString();
    }

}
