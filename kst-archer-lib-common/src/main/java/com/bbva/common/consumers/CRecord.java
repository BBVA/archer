package com.bbva.common.consumers;

import com.bbva.common.utils.OptionalRecordHeaders;
import com.bbva.common.utils.RecordHeaders;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

public class CRecord {

    public static final String TYPE_KEY = "type";
    public static final String FLAG_REPLAY_KEY = "flag.replay";

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

    public String topic() {
        return this.topic;
    }

    public String key() {
        return key;
    }

    public <V extends SpecificRecord> V value() {
        return value != null ? (V) value : null;
    }

    public int partition() {
        return this.partition;
    }

    public long offset() {
        return this.offset;
    }

    public long timestamp() {
        return this.timestamp;
    }

    public TimestampType timestampType() {
        return this.timestampType;
    }

    public RecordHeaders recordHeaders() {
        return this.headers;
    }

    public OptionalRecordHeaders optionalRecordHeaders() {
        return new OptionalRecordHeaders(this.headers.getList());
    }

    public boolean isReplayMode() {
        return this.headers.find(FLAG_REPLAY_KEY).asBoolean();
    }
}
