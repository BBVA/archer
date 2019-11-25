package com.bbva.dataprocessors.builders.dataflows.states;

import com.bbva.common.config.AppConfig;
import com.bbva.common.utils.TopicManager;
import com.bbva.common.utils.serdes.SpecificAvroSerde;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import com.bbva.dataprocessors.transformers.UniqueFieldTransformer;
import org.apache.avro.specific.SpecificRecordBase;
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
 * Builder to manage unique fields
 *
 * @param <K>  Key class type
 * @param <V>  Class type of record definition
 * @param <K1> Key class type
 */
public class UniqueFieldStateBuilder<K, V extends SpecificRecordBase, K1> implements StateDataflowBuilder {

    private DataflowProcessorContext context;
    private final Class<K> keyClass;
    private final Class<K1> key1Class;
    private final String fieldPath;
    private final String sourceTopicName;

    /**
     * Constructor
     *
     * @param sourceTopicName source base name
     * @param fieldPath       field path
     * @param keyClass        class type of the key
     * @param key1Class       class ype of second key
     */
    public UniqueFieldStateBuilder(final String sourceTopicName, final String fieldPath, final Class<K> keyClass,
                                   final Class<K1> key1Class) {
        this.sourceTopicName = sourceTopicName;
        this.fieldPath = fieldPath;
        this.keyClass = keyClass;
        this.key1Class = key1Class;
    }

    /**
     * Initialize the builder
     *
     * @param context context of the builder
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

        final Serde<K1> key1Serde = Serdes.serdeFrom(key1Class);
        final Serde<K> keySerde = Serdes.serdeFrom(keyClass);
        final SpecificAvroSerde<V> valueSerde = new SpecificAvroSerde<>(context.schemaRegistryClient(),
                context.serdeProperties());
        valueSerde.configure(context.serdeProperties(), false);

        final String sinkInternalChangelogTopicName = AppConfig.INTERNAL_NAME_PREFIX + context.applicationId()
                + AppConfig.STORE_NAME_SUFFIX + AppConfig.CHANGELOG_RECORD_NAME_SUFFIX;
        final String internalLocalStoreName = AppConfig.INTERNAL_NAME_PREFIX + context.applicationId()
                + AppConfig.STORE_NAME_SUFFIX;
        final String applicationGlobalStoreName = context.name() + AppConfig.STORE_NAME_SUFFIX;

        final Map<String, String> topics = new HashMap<>();
        topics.put(sinkInternalChangelogTopicName, AppConfig.SNAPSHOT_RECORD_TYPE);
        topics.put(sourceTopicName, AppConfig.CHANGELOG_RECORD_TYPE);
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
