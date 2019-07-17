package com.bbva.dataprocessors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;

import java.util.HashMap;
import java.util.Map;

public class InteractiveLocalDB {

    private final KafkaStreams streams;
    private final Map<String, ReadableStore> readableStores;

    public InteractiveLocalDB(final KafkaStreams streams) {
        this.streams = streams;
        this.readableStores = new HashMap<>();
    }

    public <K, V> ReadableStore<K, V> getStore(final String storeName) {
        final ReadableStore<K, V> store;
        if (readableStores.containsKey(storeName)) {
            store = readableStores.get(storeName);
        } else {
            store = new ReadableStore<>(storeName);
            readableStores.put(storeName, new ReadableStore<>(storeName));
        }
        return store;
    }

    public class ReadableStore<K, V> extends com.bbva.dataprocessors.ReadableStore {

        ReadableStore(final String storeName) {
            store = streams.store(storeName, QueryableStoreTypes.<K, V>keyValueStore());
        }

    }
}
