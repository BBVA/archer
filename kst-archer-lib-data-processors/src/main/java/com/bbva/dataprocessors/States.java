package com.bbva.dataprocessors;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.exceptions.StoreNotFoundException;
import org.apache.kafka.streams.KafkaStreams;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public final class States {

    private final Map<String, ProcessorBuilder> states = new LinkedHashMap<>();
    private final Map<String, ReadableStore> readableStores = new HashMap<>();
    private static States instance;

    public static States get() {
        if (instance == null) {
            instance = new States();
        }
        return instance;
    }

    public States add(final String name, final ProcessorBuilder builder) {
        states.put(name, builder);
        return this;
    }

    public <K, V> ReadableStore<K, V> getStore(final String name) throws StoreNotFoundException {
        final String storeName = name + ApplicationConfig.STORE_NAME_SUFFIX;
        final ReadableStore<K, V> store;

        if (readableStores.containsKey(storeName)) {
            store = readableStores.get(storeName);
        } else {
            if (states.containsKey(name)) {
                final KafkaStreams streams = states.get(name).streams();
                store = new ReadableStore<>(storeName, streams);
                readableStores.put(storeName, store);
            } else {
                throw new StoreNotFoundException("State not found :" + name);
            }
        }
        return store;
    }
}
