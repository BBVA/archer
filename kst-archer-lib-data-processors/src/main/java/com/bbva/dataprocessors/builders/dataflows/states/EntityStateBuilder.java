package com.bbva.dataprocessors.builders.dataflows.states;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.GenericClass;
import com.bbva.common.utils.TopicManager;
import com.bbva.common.utils.serdes.SpecificAvroSerde;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import com.bbva.dataprocessors.transformers.EntityTransformer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;

public class EntityStateBuilder<K, V extends SpecificRecordBase> implements StateDataflowBuilder {
    private DataflowProcessorContext context;
    private final GenericClass<K> keyClass;
    private final String snapshotTopicName;

    public EntityStateBuilder(final GenericClass<K> keyClass) {
        this.keyClass = keyClass;
        snapshotTopicName = ApplicationConfig.INTERNAL_NAME_PREFIX + context.applicationId()
                + ApplicationConfig.STORE_NAME_SUFFIX + ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX;
    }

    public EntityStateBuilder(final String snapshotTopicName, final GenericClass<K> keyClass) {
        this.keyClass = keyClass;
        this.snapshotTopicName = snapshotTopicName;
    }

    @Override
    public void init(final DataflowProcessorContext context) {
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

        final Map<String, String> topics = new HashMap<>();
        topics.put(snapshotTopicName, ApplicationConfig.SNAPSHOT_RECORD_TYPE);
        topics.put(sourceChangelogTopicName, ApplicationConfig.CHANGELOG_RECORD_TYPE);
        TopicManager.createTopics(topics, context.configs());

        final org.apache.kafka.streams.StreamsBuilder builder = context.streamsBuilder();

        final StoreBuilder<KeyValueStore<K, V>> entityStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(internalLocalStoreName), keySerde, valueSerde)
                .withLoggingEnabled(TopicManager.configTypes.get(ApplicationConfig.SNAPSHOT_RECORD_TYPE));

        builder.addStateStore(entityStore).stream(sourceChangelogTopicName, Consumed.with(keySerde, valueSerde))
                .transform(() -> new EntityTransformer<>(entityStore.name()), entityStore.name())
                .to(snapshotTopicName, Produced.with(keySerde, valueSerde));

        builder.globalTable(snapshotTopicName,
                Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(applicationGlobalStoreName).withKeySerde(keySerde)
                        .withValueSerde(valueSerde));
    }
}
