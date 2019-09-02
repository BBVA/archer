package com.bbva.dataprocessors.transformers;

import com.bbva.dataprocessors.util.ObjectUtils;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;

import java.lang.reflect.InvocationTargetException;

public class SelectForeignKeyTransformer<K, V extends SpecificRecord> extends EntityTransformer<K, V> {

    private final String foreignKeyFieldName;
    private final Class<V> valueClass;
    private static final Logger logger = LoggerFactory.getLogger(SelectForeignKeyTransformer.class);

    public SelectForeignKeyTransformer(final String entityStoreName, final String foreignKeyFieldName, final Class<V> valueClass) {
        super(entityStoreName);
        this.foreignKeyFieldName = foreignKeyFieldName;
        this.valueClass = valueClass;
    }

    @Override
    public KeyValue<K, V> transform(final K key, final V value) {
        KeyValue<K, V> resultKeyValue = null;

        final V oldValue = stateStore.get(key);
        final V newValue = oldValue != null && value != null ? (V) ObjectUtils.merge(oldValue, value) : value;
        stateStore.put(key, newValue);

        try {
            final V objectToInvoke = newValue != null ? newValue : oldValue;
            final K foreignKeyValue = (K) valueClass
                    .getMethod(getFieldNameMethod(foreignKeyFieldName, true))
                    .invoke(objectToInvoke);
            resultKeyValue = KeyValue.pair(foreignKeyValue, newValue);


        } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            logger.error(e.getMessage(), e);
        }

        return resultKeyValue;
    }

    private String getFieldNameMethod(final String fieldName, final boolean isGet) {
        return (isGet ? "get" : "set") +
                fieldName.substring(0, 1).toUpperCase() +
                fieldName.substring(1);
    }
}
