package com.bbva.dataprocessors;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.dataprocessors.builders.ProcessorBuilder;
import org.apache.kafka.streams.KafkaStreams;

import java.util.*;

public final class States {

    private Map<String, ProcessorBuilder> states = new LinkedHashMap<>();
    private Map<String, ReadableStore> readableStores = new HashMap<>();
    private static States instance = null;

    public static States get() {
        if (instance == null) {
            instance = new States();
        }
        return instance;
    }

    public States add(String name, ProcessorBuilder builder) {
        states.put(name, builder);
        return this;
    }

    public <K, V> ReadableStore<K, V> getStore(String name) {
        String storeName = name + ApplicationConfig.STORE_NAME_SUFFIX;
        ReadableStore<K, V> store;

        if (readableStores.containsKey(storeName)) {
            store = readableStores.get(storeName);
        } else {
            if (states.containsKey(name)) {
                KafkaStreams streams = states.get(name).streams();
                store = new ReadableStore<>(storeName, streams);
                readableStores.put(storeName, store);
            } else {
                throw new NullPointerException();
            }
        }
        return store;
    }
}
