package com.bbva.dataprocessors.states;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * Readable KeyValue store
 *
 * @param <K> Key class type
 * @param <V> Value class type
 */
public class ReadableStore<K, V> {

    private ReadOnlyKeyValueStore<K, V> store;

    /**
     * Constructor
     *
     * @param storeName name
     * @param streams   kafka streams instance
     */
    ReadableStore(final String storeName, final KafkaStreams streams) {
        store = streams.store(storeName, QueryableStoreTypes.keyValueStore());
    }

    /**
     * Constructor
     */
    public ReadableStore() {
    }

    /**
     * Check if key exists in the store
     *
     * @param key key to check
     * @return true/false
     */
    public boolean exists(final K key) {
        return findById(key) != null;
    }

    /**
     * Find value by key
     *
     * @param key key to find
     * @return value
     */
    public V findById(final K key) {
        return store.get(key);
    }

    /**
     * Find all records in the store
     *
     * @return iterator
     */
    public KeyValueIterator<K, V> findAll() {
        return store.all();
    }

    /**
     * Get a range of records
     *
     * @param from index from
     * @param to   index to
     * @return range iterator
     */
    public KeyValueIterator<K, V> range(final K from, final K to) {
        return store.range(from, to);
    }

    /**
     * get the actual approximate number of entries in the store
     *
     * @return count
     */
    public long approximateNumEntries() {
        return store.approximateNumEntries();
    }
}