package com.bbva.dataprocessors.builders.dataflows.states;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.TopicManager;
import com.bbva.common.utils.serdes.GenericAvroSerde;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
import java.util.Map;

public class SimpleGlobalTableStateBuilder implements TableStateBuilder {
    private final String sourceTopicName;
    private DataflowProcessorContext context;

    public SimpleGlobalTableStateBuilder(final String sourceTopicName) {
        this.sourceTopicName = sourceTopicName;
    }

    @Override
    public void init(final DataflowProcessorContext context) {
        this.context = context;
    }

    @Override
    public String sourceTopicName() {
        return this.sourceTopicName;
    }

    @Override
    public void build() {

        final GenericAvroSerde newValueSerde = new GenericAvroSerde(context.schemaRegistryClient(),
                context.serdeProperties());
        newValueSerde.configure(context.serdeProperties(), false);

        final String applicationGlobalStoreName = context.name() + ApplicationConfig.STORE_NAME_SUFFIX;

        final Map<String, Map<String, String>> topics = new HashMap<>();
        final Map<String, String> sourceTopicNameConfig = new HashMap<>();
        sourceTopicNameConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        sourceTopicNameConfig.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0");
        sourceTopicNameConfig.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0");
        topics.put(sourceTopicName, sourceTopicNameConfig);
        TopicManager.createTopics(topics, context.configs());

        final StreamsBuilder builder = context.streamsBuilder();

        builder.globalTable(sourceTopicName,
                Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>> as(applicationGlobalStoreName)
                        .withKeySerde(Serdes.String()).withValueSerde(newValueSerde));
    }
}