package com.bbva.dataprocessors.builders.dataflows.states;

import com.bbva.common.config.AppConfig;
import com.bbva.common.utils.TopicManager;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import com.bbva.dataprocessors.transformers.GroupByFieldTransformer;
import com.bbva.dataprocessors.transformers.SelectForeignKeyTransformer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
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

/**
 * Builder to manage field groups
 *
 * @param <K> Key class type
 * @param <V> Class type of record definition
 */
public class GroupByFieldStateBuilder<K, V extends SpecificRecord> implements StateDataflowBuilder {

    private DataflowProcessorContext context;
    private final String sourceChangelogTopicName;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String foreignKeyFieldName;

    /**
     * Constructor
     *
     * @param sourceChangelogTopicName source base name
     * @param keyClass                 class type of key
     * @param valueClass               class type of value record
     * @param foreignKeyFieldName      foreign field
     */
    public GroupByFieldStateBuilder(final String sourceChangelogTopicName, final Class<K> keyClass, final Class<V> valueClass, final String foreignKeyFieldName) {
        this.sourceChangelogTopicName = sourceChangelogTopicName;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.foreignKeyFieldName = foreignKeyFieldName;
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
     * Build
     */
    @Override
    public void build() {
        final Serde<K> keySerde = Serdes.serdeFrom(keyClass);
        final SpecificAvroSerde<V> valueSerde = new SpecificAvroSerde<>();
        valueSerde.configure(context.serdeProperties(), false);

        final GenericAvroSerde resultValueSerde = new GenericAvroSerde();
        resultValueSerde.configure(context.serdeProperties(), false);

        final String internalListStoreName = AppConfig.INTERNAL_NAME_PREFIX + context.applicationId()
                + AppConfig.STORE_NAME_SUFFIX;
        final String internalEntityListStoreName = AppConfig.INTERNAL_NAME_PREFIX + "entity_" + context.applicationId()
                + AppConfig.STORE_NAME_SUFFIX;
        final String internalLocalSelectKeyChangelog = AppConfig.INTERNAL_NAME_PREFIX + "selectkey_" + context.applicationId()
                + AppConfig.CHANGELOG_RECORD_NAME_SUFFIX;
        final String internalLocalStoreNameChangelog = AppConfig.INTERNAL_NAME_PREFIX + context.applicationId()
                + AppConfig.STORE_NAME_SUFFIX + AppConfig.CHANGELOG_RECORD_NAME_SUFFIX;
        final String applicationGlobalStoreName = context.name() + AppConfig.STORE_NAME_SUFFIX;

        final Map<String, String> topics = new HashMap<>();
        topics.put(internalLocalStoreNameChangelog, AppConfig.CHANGELOG_RECORD_TYPE);
        topics.put(internalLocalSelectKeyChangelog, AppConfig.CHANGELOG_RECORD_TYPE);
        topics.put(sourceChangelogTopicName, AppConfig.CHANGELOG_RECORD_TYPE);
        TopicManager.createTopics(topics, context.configs());

        final StreamsBuilder builder = context.streamsBuilder();

        final StoreBuilder<KeyValueStore<K, GenericRecord>> listStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(internalListStoreName), keySerde, resultValueSerde)
                .withLoggingEnabled(TopicManager.configTypes.get(AppConfig.SNAPSHOT_RECORD_TYPE));

        final StoreBuilder<KeyValueStore<K, V>> entityStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(internalEntityListStoreName), keySerde, valueSerde)
                .withLoggingEnabled(TopicManager.configTypes.get(AppConfig.SNAPSHOT_RECORD_TYPE));

        builder
                .addStateStore(entityStore)
                .addStateStore(listStore)
                .stream(sourceChangelogTopicName, Consumed.with(keySerde, valueSerde))
                .transform(() -> new SelectForeignKeyTransformer<>(entityStore.name(), foreignKeyFieldName, valueClass), entityStore.name())
                .through(internalLocalSelectKeyChangelog, Produced.with(keySerde, valueSerde))
                .transform(() -> new GroupByFieldTransformer<>(listStore.name(), valueClass), listStore.name())
                .to(internalLocalStoreNameChangelog, Produced.with(keySerde, resultValueSerde));

        builder
                .globalTable(
                        internalLocalStoreNameChangelog,
                        Materialized
                                .<K, GenericRecord, KeyValueStore<Bytes, byte[]>>as(applicationGlobalStoreName)
                                .withKeySerde(keySerde)
                                .withValueSerde(resultValueSerde));
    }

}
