package com.bbva.common.consumers.record;

import com.bbva.common.utils.headers.OptionalRecordHeaders;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * It's a common record and contains data and metadata.
 */
public class CRecord {

    protected final String topic;
    protected final int partition;
    protected final long offset;
    protected final long timestamp;
    protected final TimestampType timestampType;
    protected final String key;
    protected final SpecificRecord value;
    protected final RecordHeaders headers;

    /**
     * Class constructor
     *
     * @param topic         topic name
     * @param partition     partition id
     * @param offset        specific offset
     * @param timestamp     time
     * @param timestampType time type
     * @param key           key
     * @param value         value
     * @param headers       record headers
     */
    public CRecord(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType, final String key,
                   final SpecificRecord value, final RecordHeaders headers) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    /**
     * Get the topic name of the record
     *
     * @return topic name
     */
    public String topic() {
        return topic;
    }

    /**
     * Get the key of the record
     *
     * @return key of the record
     */
    public String key() {
        return key;
    }

    /**
     * Get the value of the record
     *
     * @param <V> Type of the value
     * @return value of the record
     */
    public <V extends SpecificRecord> V value() {
        return value != null ? (V) value : null;
    }

    /**
     * Get the partition of the record
     *
     * @return partition of the record
     */
    public int partition() {
        return partition;
    }

    /**
     * Get the offset of the record
     *
     * @return offset of the record
     */
    public long offset() {
        return offset;
    }

    /**
     * Get the timestamp of the record
     *
     * @return timestamp of the record
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Get the timestamp type of the record
     *
     * @return timestamp type of the record
     */
    public TimestampType timestampType() {
        return timestampType;
    }

    /**
     * Get the headers of the record
     *
     * @return headers of the record
     */
    public RecordHeaders recordHeaders() {
        return headers;
    }

    /**
     * Get the optional headers of the record
     *
     * @return optional headers of the record
     */
    public OptionalRecordHeaders optionalRecordHeaders() {
        return new OptionalRecordHeaders(headers.getList());
    }

    /**
     * Get if the record are persisted in replay mode
     *
     * @return if the record is in replay mode
     */
    public boolean isReplayMode() {
        return headers.find(CommonHeaderType.FLAG_REPLAY_KEY.getName()).asBoolean();
    }

    /**
     * Get the record type that triggered it
     *
     * @return record type which triggered this event
     */
    public String referenceRecordType() {
        return headers.find(CommonHeaderType.REFERENCE_RECORD_TYPE_KEY.getName()).asString();
    }

    /**
     * Get the record position that triggered it (topic-partition-offset)
     *
     * @return position of the record which triggered this event
     */
    public String referenceRecordPosition() {
        return headers.find(CommonHeaderType.REFERENCE_RECORD_POSITION_KEY.getName()).asString();
    }
}
