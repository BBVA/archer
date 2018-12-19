package com.bbva.dataprocessors.builders.dataflows.states;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.GenericClass;
import com.bbva.common.utils.TopicManager;
import com.bbva.common.utils.serdes.SpecificAvroSerde;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import com.bbva.dataprocessors.transformers.UniqueFieldTransformer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;

public class UniqueFieldStateBuilder<K, V extends SpecificRecordBase, K1> implements StateDataflowBuilder {
    private DataflowProcessorContext context;

    private final GenericClass<K> keyClass;
    private final GenericClass<K1> key1Class;
    private final String fieldPath;
    private final String sourceTopicName;

    public UniqueFieldStateBuilder(final String sourceTopicName, final String fieldPath, final GenericClass<K> keyClass,
                                   final GenericClass<K1> key1Class) {
        this.sourceTopicName = sourceTopicName;
        this.fieldPath = fieldPath;
        this.keyClass = keyClass;
        this.key1Class = key1Class;
    }

    @Override
    public void init(final DataflowProcessorContext context) {
        this.context = context;
    }

    @Override
    public void build() {

        final Serde<K1> key1Serde = Serdes.serdeFrom(key1Class.getType());
        final Serde<K> keySerde = Serdes.serdeFrom(keyClass.getType());
        final SpecificAvroSerde<V> valueSerde = new SpecificAvroSerde<>(context.schemaRegistryClient(),
                context.serdeProperties());
        valueSerde.configure(context.serdeProperties(), false);

        final String sinkInternalChangelogTopicName = ApplicationConfig.INTERNAL_NAME_PREFIX + context.applicationId()
                + ApplicationConfig.STORE_NAME_SUFFIX + ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX;
        final String internalLocalStoreName = ApplicationConfig.INTERNAL_NAME_PREFIX + context.applicationId()
                + ApplicationConfig.STORE_NAME_SUFFIX;
        final String applicationGlobalStoreName = context.name() + ApplicationConfig.STORE_NAME_SUFFIX;

        final Map<String, Map<String, String>> topics = new HashMap<>();
        final Map<String, String> sinkInternalChangelogTopicNameConfig = new HashMap<>();
        sinkInternalChangelogTopicNameConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        sinkInternalChangelogTopicNameConfig.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0");
        sinkInternalChangelogTopicNameConfig.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0");
        topics.put(sinkInternalChangelogTopicName, sinkInternalChangelogTopicNameConfig);
        topics.put(sourceTopicName, new HashMap<>());
        TopicManager.createTopics(topics, context.configs());

        final StoreBuilder<KeyValueStore<K1, K>> localUniqueFieldStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(internalLocalStoreName), key1Serde, keySerde)
                .withLoggingDisabled();

        final StreamsBuilder builder = context.streamsBuilder();

        builder.addStateStore(localUniqueFieldStore).stream(sourceTopicName, Consumed.with(keySerde, valueSerde))
                .transform(() -> new UniqueFieldTransformer<K, V, K1>(localUniqueFieldStore.name(), fieldPath),
                        localUniqueFieldStore.name())
                .to(sinkInternalChangelogTopicName, Produced.with(key1Serde, keySerde));

        builder.globalTable(sinkInternalChangelogTopicName,
                Materialized.<K1, K, KeyValueStore<Bytes, byte[]>>as(applicationGlobalStoreName)
                        .withKeySerde(key1Serde).withValueSerde(keySerde));
    }
}
