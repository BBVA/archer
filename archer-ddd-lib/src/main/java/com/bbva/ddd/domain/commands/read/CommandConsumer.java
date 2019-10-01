package com.bbva.ddd.domain.commands.read;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.ddd.domain.RunnableConsumer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;
import java.util.function.Consumer;

/**
 * Specific consumer for command records
 *
 * @param <V> Specific command record class
 */
public class CommandConsumer<V extends SpecificRecordBase> extends RunnableConsumer<V, CommandRecord> {

    /**
     * Constructor
     *
     * @param id                consumer id
     * @param topics            list of command topics to consume
     * @param callback          callback to manage events produced
     * @param applicationConfig configuration
     */
    public CommandConsumer(final int id, final List<String> topics, final Consumer<CommandRecord> callback,
                           final ApplicationConfig applicationConfig) {

        super(id, topics, callback, applicationConfig);
    }

    /**
     * Method invoked when a new command event is producer
     *
     * @param topic         record topic
     * @param partition     partition
     * @param offset        offset in which the record is stored
     * @param timestamp     time
     * @param timestampType time type
     * @param key           key of the record
     * @param value         data
     * @param headers       record headers
     * @return command record formed
     */
    @Override
    public CommandRecord message(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType,
                                 final String key, final V value, final RecordHeaders headers) {
        return new CommandRecord(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

}
