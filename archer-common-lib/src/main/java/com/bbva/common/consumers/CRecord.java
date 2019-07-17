package com.bbva.common.consumers;

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
        return this.topic;
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
        return this.partition;
    }

    /**
     * Get the offset of the record
     *
     * @return offset of the record
     */
    public long offset() {
        return this.offset;
    }

    /**
     * Get the timestamp of the record
     *
     * @return timestamp of the record
     */
    public long timestamp() {
        return this.timestamp;
    }

    /**
     * Get the timestamp type of the record
     *
     * @return timestamp type of the record
     */
    public TimestampType timestampType() {
        return this.timestampType;
    }

    /**
     * Get the headers of the record
     *
     * @return headers of the record
     */
    public RecordHeaders recordHeaders() {
        return this.headers;
    }

    /**
     * Get the optional headers of the record
     *
     * @return optional headers of the record
     */
    public OptionalRecordHeaders optionalRecordHeaders() {
        return new OptionalRecordHeaders(this.headers.getList());
    }

    /**
     * Get if the record are persisted in replay mode
     *
     * @return if the record is in replay mode
     */
    public boolean isReplayMode() {
        return this.headers.find(CommonHeaderType.FLAG_REPLAY_KEY).asBoolean();
    }
}
