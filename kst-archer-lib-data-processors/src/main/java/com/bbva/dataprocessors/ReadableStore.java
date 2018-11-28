package com.bbva.dataprocessors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class ReadableStore<K, V> {

    private final ReadOnlyKeyValueStore<K, V> store;

    ReadableStore(String storeName, KafkaStreams streams) {
        store = streams.store(storeName, QueryableStoreTypes.<K, V> keyValueStore());
    }

    /**
     * @param key
     *            to look for in store
     * @return true if exists key, false otherwise
     */
    public boolean exists(K key) {
        return (findById(key) != null);
    }

    /**
     * @param key
     *            to look for in store
     * @return value V binded with the key param or null
     */
    public V findById(K key) {
        return store.get(key);
    }

    /**
     * @return Iterator with all key-values in the stores
     */
    public KeyValueIterator<K, V> findAll() {
        return store.all();
    }

    /**
     * @param from
     *            key
     * @param to
     *            key
     * @return Iterator with all values in range
     */
    public KeyValueIterator<K, V> range(K from, K to) {
        return store.range(from, to);
    }

    /**
     * @return approximate number of entries
     */
    public long approximateNumEntries() {
        return store.approximateNumEntries();
    }
}
