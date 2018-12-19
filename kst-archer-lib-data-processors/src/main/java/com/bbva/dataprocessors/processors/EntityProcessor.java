package com.bbva.dataprocessors.processors;

import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

public class EntityProcessor<K, V> implements Processor<K, V> {

    private static final LoggerGen logger = LoggerGenesis.getLogger(EntityProcessor.class.getName());
    // TODO nerver used
    private ProcessorContext context;
    private KeyValueStore<K, V> stateStore;
    private final String stateStoreName;

    public EntityProcessor(final String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;

        stateStore = (KeyValueStore<K, V>) context.getStateStore(stateStoreName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(final K key, final V value) {
        final V oldValue = stateStore.get(key);
        final V newValue;
        if (oldValue == null || value == null) {
            newValue = value;
        } else {
            newValue = (V) merge(oldValue, value);
        }
        stateStore.put(key, newValue);
    }

    @Override
    public void close() {
    }

    private static Object merge(final Object lastObject, final Object newObject) {
        Object result;
        try {
            result = newObject.getClass().newInstance();

            try {
                for (final PropertyDescriptor pd : Introspector.getBeanInfo(result.getClass())
                        .getPropertyDescriptors()) {
                    if (pd.getReadMethod() != null && !"class".equals(pd.getName()) && !"schema".equals(pd.getName())) {
                        Object newValue = null;
                        Object lastValue = null;

                        try {
                            newValue = pd.getReadMethod().invoke(newObject);
                            lastValue = pd.getReadMethod().invoke(lastObject);
                        } catch (final NullPointerException | IllegalAccessException | InvocationTargetException e) {
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
                        } catch (final NullPointerException | IllegalAccessException | InvocationTargetException e) {
                            logger.error(e);
                        }
                    }
                }
            } catch (final IntrospectionException e) {
                logger.error(e);
            }
        } catch (final InstantiationException | IllegalAccessException e) {
            logger.error(e);
            result = lastObject;
        }

        return result;
    }

}
