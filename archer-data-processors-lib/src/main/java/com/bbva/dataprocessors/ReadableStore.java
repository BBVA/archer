package com.bbva.dataprocessors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class ReadableStore<K, V> {

    protected ReadOnlyKeyValueStore<K, V> store;

    ReadableStore(final String storeName, final KafkaStreams streams) {
        store = streams.store(storeName, QueryableStoreTypes.<K, V>keyValueStore());
    }

    public ReadableStore() {
    }

    public boolean exists(final K key) {
        return findById(key) != null;
    }

    public V findById(final K key) {
        return store.get(key);
    }

    public KeyValueIterator<K, V> findAll() {
        return store.all();
    }

    public KeyValueIterator<K, V> range(final K from, final K to) {
        return store.range(from, to);
    }

    public long approximateNumEntries() {
        return store.approximateNumEntries();
    }
}
