package com.bbva.ddd.domain.changelogs.read;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.RecordHeaders;
import com.bbva.ddd.domain.RunnableConsumer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;
import java.util.function.Consumer;

public class ChangelogConsumer<V extends SpecificRecordBase> extends RunnableConsumer<V, ChangelogRecord> {

    public ChangelogConsumer(int id, List<String> topics, Consumer<ChangelogRecord> callback,
            ApplicationConfig applicationConfig) {
        super(id, topics, callback, applicationConfig);
    }

    @Override
    public ChangelogRecord message(String topic, int partition, long offset, long timestamp,
            TimestampType timestampType, String key, V value, RecordHeaders headers) {
        return new ChangelogRecord(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

}