package com.bbva.dataprocessors.transformers;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

public class EntityTransformer<K, V> implements Transformer<K, V, KeyValue<K, V>> {

    private ProcessorContext context;
    private KeyValueStore<K, V> stateStore;
    private String stateStoreName;

    public EntityTransformer(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context;

        // call this processor's punctuate() method every 1000 time units.
        // this.context.schedule(1000);

        stateStore = (KeyValueStore<K, V>) context.getStateStore(stateStoreName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public KeyValue<K, V> transform(final K key, final V value) {
        V oldValue = stateStore.get(key);
        V newValue;
        if (oldValue == null || value == null) {
            newValue = value;
        } else {
            newValue = (V) merge(oldValue, value);
        }
        stateStore.put(key, newValue);
        return KeyValue.pair(key, newValue);
    }

    @Override
    public void close() {
        // close the key-value store
        // stateStore.close();
    }

    private Object merge(Object lastObject, Object newObject) {
        Object result;
        try {
            result = newObject.getClass().newInstance();

            try {
                for (PropertyDescriptor pd : Introspector.getBeanInfo(result.getClass()).getPropertyDescriptors()) {
                    if (pd.getReadMethod() != null && !"class".equals(pd.getName()) && !"schema".equals(pd.getName())) {
                        Object newValue = null;
                        Object lastValue = null;

                        try {
                            newValue = pd.getReadMethod().invoke(newObject);
                            lastValue = pd.getReadMethod().invoke(lastObject);
                        } catch (NullPointerException | IllegalAccessException | InvocationTargetException e) {
                            // ignore
                        }

                        try {
                            if (newValue == null) {
                                pd.getWriteMethod().invoke(result, lastValue);
                            } else if (newValue instanceof SpecificRecord) {
                                pd.getWriteMethod().invoke(result, merge(lastValue, newValue));
                            } else {
                                pd.getWriteMethod().invoke(result, newValue);
                            }
                        } catch (NullPointerException | IllegalAccessException | InvocationTargetException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (IntrospectionException e) {
                e.printStackTrace();
            }
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            result = lastObject;
        }

        return result;
    }

}