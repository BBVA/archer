package com.bbva.dataprocessors.builders.dataflows.states;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.GenericClass;
import com.bbva.common.utils.TopicManager;
import com.bbva.common.utils.serdes.SpecificAvroSerde;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import com.bbva.dataprocessors.transformers.EntityTransformer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class EntityStateBuilder<K, V extends SpecificRecordBase> implements StateDataflowBuilder {
    private final Logger logger;
    private DataflowProcessorContext context;
    private final GenericClass<K> keyClass;
    private final String snapshotTopicName;

    public EntityStateBuilder(GenericClass<K> keyClass) {
        logger = Logger.getLogger(EntityStateBuilder.class);
        this.keyClass = keyClass;
        snapshotTopicName = ApplicationConfig.INTERNAL_NAME_PREFIX + context.applicationId()
                + ApplicationConfig.STORE_NAME_SUFFIX + ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX;
    }

    public EntityStateBuilder(String snapshotTopicName, GenericClass<K> keyClass) {
        logger = Logger.getLogger(EntityStateBuilder.class);
        this.keyClass = keyClass;
        this.snapshotTopicName = snapshotTopicName;
    }

    @Override
    public void init(DataflowProcessorContext context) {
        this.context = context;
    }

    @Override
    public void build() {
        final Serde<K> keySerde = Serdes.serdeFrom(keyClass.getType());
        final SpecificAvroSerde<V> valueSerde = new SpecificAvroSerde<>(context.schemaRegistryClient(),
                context.serdeProperties());
        valueSerde.configure(context.serdeProperties(), false);

        final String sourceChangelogTopicName = context.name() + ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX;
        final String internalLocalStoreName = ApplicationConfig.INTERNAL_NAME_PREFIX + context.applicationId()
                + ApplicationConfig.STORE_NAME_SUFFIX;
        final String applicationGlobalStoreName = context.name() + ApplicationConfig.STORE_NAME_SUFFIX;

        Map<String, Map<String, String>> topics = new HashMap<>();
        Map<String, String> snapshotTopicNameConfig = new HashMap<>();
        snapshotTopicNameConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        snapshotTopicNameConfig.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0");
        snapshotTopicNameConfig.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0");
        topics.put(snapshotTopicName, snapshotTopicNameConfig);
        topics.put(sourceChangelogTopicName, new HashMap<>());
        TopicManager.createTopics(topics, context.configs());

        org.apache.kafka.streams.StreamsBuilder builder = context.streamsBuilder();

        StoreBuilder<KeyValueStore<K, V>> entityStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(internalLocalStoreName), keySerde, valueSerde)
                .withLoggingEnabled(snapshotTopicNameConfig);

        builder.addStateStore(entityStore).stream(sourceChangelogTopicName, Consumed.with(keySerde, valueSerde))
                .transform(() -> new EntityTransformer<>(entityStore.name()), entityStore.name())
                .to(snapshotTopicName, Produced.with(keySerde, valueSerde));

        builder.globalTable(snapshotTopicName,
                Materialized.<K, V, KeyValueStore<Bytes, byte[]>> as(applicationGlobalStoreName).withKeySerde(keySerde)
                        .withValueSerde(valueSerde));
    }
}
