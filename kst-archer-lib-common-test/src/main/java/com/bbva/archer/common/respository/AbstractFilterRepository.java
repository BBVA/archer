package com.bbva.archer.common.respository;


import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.CustomCachedSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Collections;
import java.util.Date;


public abstract class AbstractFilterRepository<T extends SpecificRecord> {

    private static final LoggerGen LOGGER = LoggerGenesis.getLogger(AbstractFilterRepository.class.getName());

    private final KafkaStreams streams;
    private final KTable<String, T> table;

    public AbstractFilterRepository(final ApplicationConfig appConfig, final String topicName) {

        final StreamsBuilder builder = new StreamsBuilder();

        final String schemaUri = (String) appConfig.get().get(ApplicationConfig.SCHEMA_REGISTRY_URL);
        final SpecificAvroSerde<T> valueSerde = new SpecificAvroSerde(
                new CustomCachedSchemaRegistryClient(schemaUri, 100));
        valueSerde.configure(Collections.singletonMap(ApplicationConfig.SCHEMA_REGISTRY_URL, schemaUri), false);

        table = builder.table(topicName,
                Consumed.with(Serdes.String(), valueSerde),
                Materialized.as(topicName + "-table"));

        final String streamPreffix = "-" + new Date().getTime();
        appConfig.streams().get().put(ApplicationConfig.StreamsProperties.GROUP_ID, appConfig.streams().get().get(ApplicationConfig.StreamsProperties.GROUP_ID) + streamPreffix);
        appConfig.streams().get().put(ApplicationConfig.StreamsProperties.CLIENT_ID, appConfig.streams().get().get(ApplicationConfig.StreamsProperties.CLIENT_ID) + streamPreffix);
        appConfig.streams().get().put(ApplicationConfig.StreamsProperties.APPLICATION_ID, appConfig.streams().get().get(ApplicationConfig.StreamsProperties.APPLICATION_ID) + streamPreffix);
        appConfig.streams().get().put(ApplicationConfig.StreamsProperties.STATE_DIR, "/tmp/kafka-streams" + streamPreffix);

        streams = new KafkaStreams(builder.build(), appConfig.streams().get());

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public T findByKey(final String hashKey) {

        ReadOnlyKeyValueStore<String, T> view;
        boolean retry = true;
        while (retry) {
            try {
                if (streams != null && streams.state() == KafkaStreams.State.RUNNING) {
                    view = streams.store(table.queryableStoreName(), QueryableStoreTypes.keyValueStore());
                    final KeyValueIterator<String, T> range = view.all();
                    while (range.hasNext()) {
                        final KeyValue<String, T> next = range.next();
                        if (compareKey(next.value, hashKey)) {
                            return next.value;
                        }
                    }
                    retry = false;
                }
            } catch (final InvalidStateStoreException ignored) {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException e) {
                    LOGGER.error("Problem sleep the thread", e);
                }
                retry = true;
            }
        }
        return null;
    }

    protected abstract boolean compareKey(T value, String key);

    public void close() {
        streams.close();
        streams.cleanUp();
    }
}
