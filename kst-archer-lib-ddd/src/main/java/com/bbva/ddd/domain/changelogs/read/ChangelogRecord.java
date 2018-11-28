package com.bbva.ddd.domain.changelogs.read;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.utils.RecordHeaders;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.record.TimestampType;

public class ChangelogRecord extends CRecord {

    public static final String TYPE_CHANGELOG_VALUE = "changelog";
    public static final String UUID_KEY = "uuid";
    public static final String TRIGGER_REFERENCE_KEY = "trigger.reference";
    public static final String AGGREGATE_UUID_KEY = "aggregate.uuid";
    public static final String AGGREGATE_NAME_KEY = "aggregate.name";
    public static final String AGGREGATE_METHOD_KEY = "aggregate.method";

    public ChangelogRecord(String topic, int partition, long offset, long timestamp, TimestampType timestampType,
            String key, SpecificRecord value, RecordHeaders headers) {
        super(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    public String uuid() {
        return headers.find(ChangelogRecord.UUID_KEY).asString();
    }

    public String triggerReference() {
        return headers.find(ChangelogRecord.TRIGGER_REFERENCE_KEY).asString();
    }

    public String aggregateUuid() {
        return headers.find(ChangelogRecord.AGGREGATE_UUID_KEY).asString();
    }

    public String aggregateName() {
        return headers.find(ChangelogRecord.AGGREGATE_NAME_KEY).asString();
    }

    public String aggregateMethod() {
        return headers.find(ChangelogRecord.AGGREGATE_METHOD_KEY).asString();
    }

}
