package com.bbva.dataprocessors.transformers;

import com.bbva.dataprocessors.util.ObjectUtils;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;

import java.lang.reflect.InvocationTargetException;

/**
 * Transform to select by foreign key in a state
 *
 * @param <K> Key class
 * @param <V> Value class
 */
public class SelectForeignKeyTransformer<K, V extends SpecificRecord> extends EntityTransformer<K, V> {

    private final String foreignKeyFieldName;
    private final Class<V> valueClass;
    private static final Logger logger = LoggerFactory.getLogger(SelectForeignKeyTransformer.class);

    /**
     * Constructor
     *
     * @param entityStoreName     state store
     * @param foreignKeyFieldName field name of FK
     * @param valueClass          Class type of value
     */
    public SelectForeignKeyTransformer(final String entityStoreName, final String foreignKeyFieldName, final Class<V> valueClass) {
        super(entityStoreName);
        this.foreignKeyFieldName = foreignKeyFieldName;
        this.valueClass = valueClass;
    }

    /**
     * Index by the FK field
     *
     * @param key   record key
     * @param value record value
     * @return new keyvalue pair
     */
    @Override
    public KeyValue<K, V> transform(final K key, final V value) {
        KeyValue<K, V> resultKeyValue = null;

        final V oldValue = stateStore.get(key);
        final V newValue = oldValue != null && value != null ? (V) ObjectUtils.merge(oldValue, value) : value;
        stateStore.put(key, newValue);

        try {
            final V objectToInvoke = newValue != null ? newValue : oldValue;
            final K foreignKeyValue = (K) valueClass
                    .getMethod(ObjectUtils.getFieldNameMethod(foreignKeyFieldName, true))
                    .invoke(objectToInvoke);
            resultKeyValue = KeyValue.pair(foreignKeyValue, newValue);


        } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            logger.error(e.getMessage(), e);
        }

        return resultKeyValue;
    }
    
}
