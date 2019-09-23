package com.bbva.dataprocessors.builders.dataflows.states;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.TopicManager;
import com.bbva.common.utils.serdes.GenericAvroSerde;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder to manage state as global tables
 *
 * @param <K> Key class type
 * @param <V> Class type of record definition
 */
public class SimpleGlobalTableStateBuilder implements TableStateBuilder {
    private final String sourceTopicName;
    private DataflowProcessorContext context;

    /**
     * Constructor
     *
     * @param sourceTopicName soruce base name
     */
    public SimpleGlobalTableStateBuilder(final String sourceTopicName) {
        this.sourceTopicName = sourceTopicName;
    }

    /**
     * Initialize the builder
     *
     * @param context builder context
     */
    @Override
    public void init(final DataflowProcessorContext context) {
        this.context = context;
    }

    /**
     * Get the source base name
     *
     * @return source base name
     */
    @Override
    public String sourceTopicName() {
        return sourceTopicName;
    }

    /**
     * Build
     */
    @Override
    public void build() {

        final GenericAvroSerde newValueSerde = new GenericAvroSerde(context.schemaRegistryClient(),
                context.serdeProperties());
        newValueSerde.configure(context.serdeProperties(), false);

        final String applicationGlobalStoreName = context.name() + ApplicationConfig.STORE_NAME_SUFFIX;

        final Map<String, String> topics = new HashMap<>();
        topics.put(sourceTopicName, ApplicationConfig.SNAPSHOT_RECORD_TYPE);
        TopicManager.createTopics(topics, context.configs());

        final StreamsBuilder builder = context.streamsBuilder();

        builder.globalTable(sourceTopicName,
                Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as(applicationGlobalStoreName)
                        .withKeySerde(Serdes.String()).withValueSerde(newValueSerde));
    }
}
