package com.bbva.ddd.domain.events.read;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.ddd.domain.RunnableConsumer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;
import java.util.function.Consumer;

public class EventConsumer<V extends SpecificRecordBase> extends RunnableConsumer<V, EventRecord> {

    public EventConsumer(int id, List<String> topics, Consumer<EventRecord> callback,
                         ApplicationConfig applicationConfig) {
        super(id, topics, callback, applicationConfig);
    }

    @Override
    public EventRecord message(String topic, int partition, long offset, long timestamp, TimestampType timestampType,
                               String key, V value, RecordHeaders headers) {
        return new EventRecord(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

}