package com.bbva.ddd.domain.changelogs.read;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * Wrap records of kind changelog and offer methods to parse changelog type headers.
 */
public class ChangelogRecord extends CRecord {

    /**
     * Constructor
     *
     * @param topic         topic name
     * @param partition     partition in which the record is store
     * @param offset        offset to find the element
     * @param timestamp     time
     * @param timestampType time type
     * @param key           key off the record
     * @param value         data of record
     * @param headers       headers associates
     */
    public ChangelogRecord(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType,
                           final String key, final SpecificRecord value, final RecordHeaders headers) {
        super(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    /**
     * Get the uuid of the changelog record. It should be the same as entityUuid in CommandRecord
     *
     * @return uuid of the changelog record
     */
    public String uuid() {
        return headers.find(ChangelogHeaderType.UUID_KEY).asString();
    }

    /**
     * Get the record that triggered it
     *
     * @return record which triggered this event
     */
    public CRecord referenceRecord() {
        return (CRecord) headers.find(ChangelogHeaderType.REFERENCE_RECORD_KEY).as();
    }

    /**
     * Get the aggregate identifier
     *
     * @return aggregate identifier of the changelog record
     */
    public String aggregateUuid() {
        return headers.find(ChangelogHeaderType.AGGREGATE_UUID_KEY).asString();
    }

    /**
     * Get the name of the aggregate
     *
     * @return name of the aggregate of the changelog record
     */
    public String aggregateName() {
        return headers.find(ChangelogHeaderType.AGGREGATE_NAME_KEY).asString();
    }

    /**
     * Get the method of the aggregate
     *
     * @return method of the aggregate of the changelog record
     */
    public String aggregateMethod() {
        return headers.find(ChangelogHeaderType.AGGREGATE_METHOD_KEY).asString();
    }

}
