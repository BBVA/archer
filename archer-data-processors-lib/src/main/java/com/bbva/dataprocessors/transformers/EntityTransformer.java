package com.bbva.dataprocessors.transformers;

import com.bbva.dataprocessors.util.ObjectUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Transformer that add/merge entities in a state store
 *
 * @param <K> Key class
 * @param <V> Value class
 */
public class EntityTransformer<K, V> implements Transformer<K, V, KeyValue<K, V>> {

    protected ProcessorContext context;
    protected KeyValueStore<K, V> stateStore;
    private final String stateStoreName;

    /**
     * Constructor
     *
     * @param stateStoreName state store
     */
    public EntityTransformer(final String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore<K, V>) this.context.getStateStore(stateStoreName);
    }

    /**
     * Add or merge entity values
     */
    @Override
    public KeyValue<K, V> transform(final K key, final V value) {
        final V oldValue = stateStore.get(key);
        return setMergedKeyValue(key, ObjectUtils.getNewMergedValue(oldValue, value));
    }

    protected KeyValue<K, V> setMergedKeyValue(final K key, final V newValue) {
        stateStore.put(key, newValue);
        return KeyValue.pair(key, newValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        //Do nothing
    }

}
