package com.bbva.dataprocessors.processors;

import com.bbva.dataprocessors.util.ObjectUtils;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Entity processor
 *
 * @param <K> key class
 * @param <V> value class
 */
public class EntityProcessor<K, V> implements Processor<K, V> {

    private KeyValueStore<K, V> stateStore;
    private final String stateStoreName;

    /**
     * Constructor
     *
     * @param stateStoreName store name
     */
    public EntityProcessor(final String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    /**
     * Initialize the processor
     *
     * @param context processor context
     */
    @Override
    public void init(final ProcessorContext context) {
        stateStore = (KeyValueStore<K, V>) context.getStateStore(stateStoreName);
    }

    /**
     * Process record
     *
     * @param key   record key
     * @param value value key
     */
    @Override
    public void process(final K key, final V value) {
        final V oldValue = stateStore.get(key);
        stateStore.put(key, ObjectUtils.getNewMergedValue(oldValue, value));
    }

    /**
     * Close processor
     */
    @Override
    public void close() {
        //Do nothing
    }
}
