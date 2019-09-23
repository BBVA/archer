package com.bbva.dataprocessors.transformers;

import com.bbva.dataprocessors.util.ObjectUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class EntityTransformer<K, V> implements Transformer<K, V, KeyValue<K, V>> {

    protected ProcessorContext context;
    protected KeyValueStore<K, V> stateStore;
    private final String stateStoreName;

    public EntityTransformer(final String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;

        stateStore = (KeyValueStore<K, V>) this.context.getStateStore(stateStoreName);
    }

    @Override
    public KeyValue<K, V> transform(final K key, final V value) {
        final V oldValue = stateStore.get(key);
        final V newValue;
        if (oldValue == null || value == null) {
            newValue = value;
        } else {
            newValue = (V) ObjectUtils.merge(oldValue, value);
        }
        return setMergedKeyValue(key, newValue);
    }

    protected KeyValue<K, V> setMergedKeyValue(final K key, final V newValue) {
        stateStore.put(key, newValue);
        return KeyValue.pair(key, newValue);
    }

    @Override
    public void close() {
        //Do nothing
    }

}
