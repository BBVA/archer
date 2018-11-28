package com.bbva.ddd.domain.commands.read;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.RecordHeaders;
import com.bbva.ddd.domain.RunnableConsumer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;
import java.util.function.Consumer;

public class CommandConsumer<V extends SpecificRecordBase> extends RunnableConsumer<V, CommandRecord> {

    public CommandConsumer(int id, List<String> topics, Consumer<CommandRecord> callback,
            ApplicationConfig applicationConfig) {
        super(id, topics, callback, applicationConfig);
    }

    @Override
    public CommandRecord message(String topic, int partition, long offset, long timestamp, TimestampType timestampType,
            String key, V value, RecordHeaders headers) {
        return new CommandRecord(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

}