package com.bbva.ddd.domain.changelogs.read;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.ddd.domain.RunnableConsumer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;
import java.util.function.Consumer;

/**
 * Specific consumer for changelog records
 *
 * @param <V> Specific changelog record class
 */
public class ChangelogConsumer<V extends SpecificRecordBase> extends RunnableConsumer<V, ChangelogRecord> {

    /**
     * Constructor
     *
     * @param id                id of the consumer
     * @param topics            list of topics to consume
     * @param callback          callback to manage events produced
     * @param applicationConfig configuration
     */
    public ChangelogConsumer(final int id, final List<String> topics, final Consumer<ChangelogRecord> callback,
                             final ApplicationConfig applicationConfig) {
        super(id, topics, callback, applicationConfig);
    }

    /**
     * Method invoked when a new changelog event is producer
     *
     * @param topic         record topic
     * @param partition     partition
     * @param offset        offset in which the record is stored
     * @param timestamp     time
     * @param timestampType time type
     * @param key           key of the record
     * @param value         data
     * @param headers       record headers
     * @return changelog record formed
     */
    @Override
    public ChangelogRecord message(final String topic, final int partition, final long offset, final long timestamp,
                                   final TimestampType timestampType, final String key, final V value, final RecordHeaders headers) {
        return new ChangelogRecord(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

}
