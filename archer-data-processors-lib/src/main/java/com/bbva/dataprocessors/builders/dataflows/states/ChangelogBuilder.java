package com.bbva.dataprocessors.builders.dataflows.states;

import com.bbva.common.config.AppConfig;
import com.bbva.common.utils.TopicManager;
import com.bbva.common.utils.serdes.SpecificAvroSerde;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import com.bbva.dataprocessors.transformers.EntityTransformer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;

import static com.bbva.common.config.AppConfig.CHANGELOG_RECORD_NAME_SUFFIX;

/**
 * Builder to manage internal changelogs
 *
 * @param <K> Key class type
 * @param <V> Class type of record definition
 */
public abstract class ChangelogBuilder<K, V extends SpecificRecordBase> implements StateDataflowBuilder {
    private static final String SNAPSHOT = "_snapshot";
    private static final String STORE = "_store";
    private DataflowProcessorContext context;
    private final Class<K> keyClass;
    protected final String snapshotTopicName;
    private final String topic;

    /**
     * Constructor
     *
     * @param baseName base name
     */
    public ChangelogBuilder(final String baseName) {
        keyClass = (Class<K>) String.class;
        snapshotTopicName = baseName.concat(SNAPSHOT);
        topic = baseName;
    }

    /**
     * Constructor
     *
     * @param baseName base name
     * @param topic    internal topic
     */
    public ChangelogBuilder(final String baseName, final String topic) {
        keyClass = (Class<K>) String.class;
        snapshotTopicName = baseName.concat(SNAPSHOT);
        this.topic = topic;
    }

    /**
     * Initialize builder
     *
     * @param context dataflow context
     */
    @Override
    public void init(final DataflowProcessorContext context) {
        this.context = context;
    }

    /**
     * Build the builder
     */
    @Override
    public void build() {
        final Serde<K> keySerde = Serdes.serdeFrom(keyClass);
        final SpecificAvroSerde<V> valueSerde = new SpecificAvroSerde<>(context.schemaRegistryClient(), context.serdeProperties());
        valueSerde.configure(context.serdeProperties(), false);
        final String sourceChangelogTopicName = topic + CHANGELOG_RECORD_NAME_SUFFIX;
        final String internalLocalStoreName = "internal_" + context.applicationId() + STORE;
        final String applicationGlobalStoreName = context.name() + STORE;

        createTopics(sourceChangelogTopicName);

        final StreamsBuilder builder = context.streamsBuilder();
        final StoreBuilder<KeyValueStore<K, V>> entityStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(internalLocalStoreName), keySerde, valueSerde)
                .withLoggingEnabled(TopicManager.configTypes.get(AppConfig.SNAPSHOT_RECORD_TYPE));

        builder
                .addStateStore(entityStore)
                .stream(sourceChangelogTopicName, Consumed.with(keySerde, valueSerde))
                .transform(() -> newTransformer(entityStore.name()), new String[]{entityStore.name()})
                .to(snapshotTopicName, Produced.with(keySerde, valueSerde));

        addTable(builder, applicationGlobalStoreName, valueSerde);
    }

    /**
     * Add global table to builder
     *
     * @param builder    streams builder
     * @param name       table name
     * @param valueSerde value serde
     */
    protected void addTable(final StreamsBuilder builder, final String name, final SpecificAvroSerde<V> valueSerde) {
        builder.globalTable(snapshotTopicName,
                Consumed.with(Serdes.String(), valueSerde),
                Materialized.as(name));
    }

    /**
     * Set transformer
     *
     * @param entityName name of the entity
     * @return transformer
     */
    protected abstract EntityTransformer<K, V> newTransformer(final String entityName);

    private void createTopics(final String sourceChangelogTopicName) {
        final Map<String, String> topics = new HashMap<>();
        topics.put(snapshotTopicName, AppConfig.SNAPSHOT_RECORD_TYPE);
        topics.put(sourceChangelogTopicName, AppConfig.CHANGELOG_RECORD_TYPE);
        TopicManager.createTopics(topics, context.configs());
    }
}
