package com.bbva.dataprocessors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;

import java.util.HashMap;
import java.util.Map;

/**
 * Interactive db local with readable stores
 */
public class InteractiveLocalDB {

    private final KafkaStreams streams;
    private final Map<String, ReadableStore> readableStores;

    /**
     * Constructor
     *
     * @param streams kafka streams instance
     */
    public InteractiveLocalDB(final KafkaStreams streams) {
        this.streams = streams;
        readableStores = new HashMap<>();
    }

    /**
     * Get readable store
     *
     * @param storeName name
     * @param <K>       Key class
     * @param <V>       Value class
     * @return store
     */
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

    /**
     * Readable store
     *
     * @param <K> Key class
     * @param <V> Value class
     */
    public class ReadableStore<K, V> extends com.bbva.dataprocessors.ReadableStore {

        /**
         * Constructor
         *
         * @param storeName store name
         */
        ReadableStore(final String storeName) {
            super();
            store = streams.store(storeName, QueryableStoreTypes.<K, V>keyValueStore());
        }

    }
}
