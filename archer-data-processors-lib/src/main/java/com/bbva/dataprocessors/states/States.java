package com.bbva.dataprocessors.states;

import com.bbva.common.config.AppConfig;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.exceptions.StoreNotFoundException;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * States, stores and processors
 */
public final class States {

    private static final Logger logger = LoggerFactory.getLogger(States.class);

    private final Map<String, ProcessorBuilder> states = new LinkedHashMap<>();
    private final Map<String, ReadableStore> readableStores = new HashMap<>();
    private static States instance;

    /**
     * Create/Get a states instance
     *
     * @return instance
     */
    public static States get() {
        if (instance == null) {
            instance = new States();
        }
        return instance;
    }

    /**
     * Add processor builder to states
     *
     * @param name    processor name
     * @param builder processor builder
     * @return states instance
     */
    public States add(final String name, final ProcessorBuilder builder) {
        states.put(name, builder);
        return this;
    }

    /**
     * Get store by name from states
     *
     * @param name store name
     * @param <K>  Key class
     * @param <V>  Value class
     * @return store
     * @throws StoreNotFoundException if store not exists
     */
    public <K, V> ReadableStore<K, V> getStore(final String name) throws StoreNotFoundException {
        final String storeName = name + AppConfig.STORE_NAME_SUFFIX;
        ReadableStore<K, V> store;

        while (true) {
            try {
                if (readableStores.containsKey(storeName)) {
                    store = readableStores.get(storeName);
                } else if (states.containsKey(name)) {
                    final KafkaStreams streams = states.get(name).streams();
                    store = new ReadableStore<>(storeName, streams);
                    readableStores.put(storeName, store);
                } else {
                    throw new StoreNotFoundException("State not found :" + name);
                }

                return store;
            } catch (final InvalidStateStoreException ignored) {
                try {
                    Thread.sleep(500);
                } catch (final InterruptedException e) { //NOSONAR
                    logger.error("Problems sleeping the execution", e);
                    throw new ApplicationException("Problems sleeping the execution", e);
                }
            }
        }
    }

    /**
     * Gte the state of the store
     *
     * @param name store name
     * @return state
     */
    public KafkaStreams.State getStoreState(final String name) {
        final KafkaStreams streams = states.get(name).streams();
        return streams.state();
    }
}
