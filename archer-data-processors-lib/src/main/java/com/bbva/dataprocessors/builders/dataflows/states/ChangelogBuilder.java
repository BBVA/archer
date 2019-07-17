package com.bbva.dataprocessors.builders.dataflows.states;


import com.bbva.common.config.ApplicationConfig;
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

import static com.bbva.common.config.ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX;

public abstract class ChangelogBuilder<K, V extends SpecificRecordBase> implements StateDataflowBuilder {
    public static final String SNAPSHOT = "_snapshot";
    public static final String STORE = "_store";
    private DataflowProcessorContext context;
    private final Class<K> keyClass;
    protected final String snapshotTopicName;
    private final String topic;

    public ChangelogBuilder(final String baseName) {
        this.keyClass = (Class<K>) String.class;
        this.snapshotTopicName = baseName + SNAPSHOT;
        this.topic = baseName;
    }

    public ChangelogBuilder(final String baseName, final String topic) {
        this.keyClass = (Class<K>) String.class;
        this.snapshotTopicName = baseName + SNAPSHOT;
        this.topic = topic;
    }

    @Override
    public void init(final DataflowProcessorContext context) {
        this.context = context;
    }

    @Override
    public void build() {
        final Serde<K> keySerde = Serdes.serdeFrom(this.keyClass);
        final SpecificAvroSerde<V> valueSerde = new SpecificAvroSerde<>(this.context.schemaRegistryClient(), this.context.serdeProperties());
        valueSerde.configure(this.context.serdeProperties(), false);
        final String sourceChangelogTopicName = this.topic + CHANGELOG_RECORD_NAME_SUFFIX;
        final String internalLocalStoreName = "internal_" + this.context.applicationId() + STORE;
        final String applicationGlobalStoreName = this.context.name() + STORE;

        createTopics(sourceChangelogTopicName);

        final StreamsBuilder builder = this.context.streamsBuilder();
        final StoreBuilder<KeyValueStore<K, V>> entityStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(internalLocalStoreName), keySerde, valueSerde)
                .withLoggingEnabled(TopicManager.configTypes.get(ApplicationConfig.SNAPSHOT_RECORD_TYPE));

        builder
                .addStateStore(entityStore)
                .stream(sourceChangelogTopicName, Consumed.with(keySerde, valueSerde))
                .transform(() -> newTransformer(entityStore.name()), new String[]{entityStore.name()})
                .to(this.snapshotTopicName, Produced.with(keySerde, valueSerde));

        addTable(builder, applicationGlobalStoreName, valueSerde);
    }

    protected void addTable(final StreamsBuilder builder, final String name, final SpecificAvroSerde<V> valueSerde) {
        builder.globalTable(this.snapshotTopicName,
                Consumed.with(Serdes.String(), valueSerde),
                Materialized.as(name));
    }

    protected abstract EntityTransformer<K, V> newTransformer(final String entityName);

    private void createTopics(final String sourceChangelogTopicName) {
        final Map<String, String> topics = new HashMap<>();
        topics.put(this.snapshotTopicName, ApplicationConfig.SNAPSHOT_RECORD_TYPE);
        topics.put(sourceChangelogTopicName, ApplicationConfig.CHANGELOG_RECORD_TYPE);
        TopicManager.createTopics(topics, this.context.configs());
    }
}
