package com.bbva.dataprocessors.processors;

import com.bbva.dataprocessors.util.ObjectUtils;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class EntityProcessor<K, V> implements Processor<K, V> {

    private ProcessorContext context;
    private KeyValueStore<K, V> stateStore;
    private final String stateStoreName;

    public EntityProcessor(final String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;

        stateStore = (KeyValueStore<K, V>) this.context.getStateStore(stateStoreName);
    }

    @Override
    public void process(final K key, final V value) {
        final V oldValue = stateStore.get(key);
        final V newValue;
        if (oldValue == null || value == null) {
            newValue = value;
        } else {
            newValue = (V) ObjectUtils.merge(oldValue, value);
        }
        stateStore.put(key, newValue);
    }

    @Override
    public void close() {
    }
}
