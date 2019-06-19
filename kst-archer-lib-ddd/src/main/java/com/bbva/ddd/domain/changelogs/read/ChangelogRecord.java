package com.bbva.ddd.domain.changelogs.read;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

public class ChangelogRecord extends CRecord {

    public ChangelogRecord(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType,
                           final String key, final SpecificRecord value, final RecordHeaders headers) {
        super(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    public String uuid() {
        return headers.find(ChangelogHeaderType.UUID_KEY).asString();
    }

    public String triggerReference() {
        return headers.find(ChangelogHeaderType.TRIGGER_REFERENCE_KEY).asString();
    }

    public String aggregateUuid() {
        return headers.find(ChangelogHeaderType.AGGREGATE_UUID_KEY).asString();
    }

    public String aggregateName() {
        return headers.find(ChangelogHeaderType.AGGREGATE_NAME_KEY).asString();
    }

    public String aggregateMethod() {
        return headers.find(ChangelogHeaderType.AGGREGATE_METHOD_KEY).asString();
    }

}
