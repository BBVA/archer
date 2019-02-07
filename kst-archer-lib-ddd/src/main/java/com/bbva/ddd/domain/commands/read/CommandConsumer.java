package com.bbva.ddd.domain.commands.read;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.RecordHeaders;
import com.bbva.ddd.domain.RunnableConsumer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;
import java.util.function.Consumer;

public class CommandConsumer<V extends SpecificRecordBase> extends RunnableConsumer<V, CommandRecord> {

    public CommandConsumer(final int id, final List<String> topics, final Consumer<CommandRecord> callback,
                           final ApplicationConfig applicationConfig) {

        super(id, topics, callback, applicationConfig);
    }

    @Override
    public CommandRecord message(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType,
                                 final String key, final V value, final RecordHeaders headers) {
        return new CommandRecord(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

}
