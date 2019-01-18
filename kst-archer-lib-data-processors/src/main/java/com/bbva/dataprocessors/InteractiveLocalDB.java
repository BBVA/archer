package com.bbva.dataprocessors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

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

    public class ReadableStore<K, V> {

        private final ReadOnlyKeyValueStore<K, V> store;

        ReadableStore(final String storeName) {
            store = streams.store(storeName, QueryableStoreTypes.<K, V>keyValueStore());
        }

        /**
         * @param key to look for in store
         * @return true if exists key, false otherwise
         */
        public boolean exists(final K key) {
            return findById(key) != null;
        }

        /**
         * @param key to look for in store
         * @return value V binded with the key param or null
         */
        public V findById(final K key) {
            return store.get(key);
        }

        /**
         * @return Iterator with all key-values in the stores
         */
        public KeyValueIterator<K, V> findAll() {
            return store.all();
        }

        /**
         * @param from key
         * @param to   key
         * @return Iterator with all values in range
         */
        public KeyValueIterator<K, V> range(final K from, final K to) {
            return store.range(from, to);
        }

        /**
         * @return approximate number of entries
         */
        public long approximateNumEntries() {
            return store.approximateNumEntries();
        }
    }
}
