package com.bbva.gateway.consumer.transformer;


import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import com.bbva.dataprocessors.transformers.EntityTransformer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.KeyValue;

/**
 * Transformer for gateway changelog
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class ChangelogTransformer<K, V> extends EntityTransformer<K, V> {

    /**
     * Constructor
     *
     * @param stateStoreName state store
     */
    public ChangelogTransformer(final String stateStoreName) {
        super(stateStoreName);
    }

    /**
     * Set reference record as new key form search
     *
     * @param key      record key
     * @param newValue value key
     * @return new key/value pair
     */
    @Override
    protected KeyValue<K, V> setMergedKeyValue(final K key, final V newValue) {
        final Header header = context.headers().lastHeader(ChangelogHeaderType.REFERENCE_RECORD_KEY.getName());
        if (header != null) {
            final String newKey = new ByteArrayValue(header.value()).asString();
            stateStore.put((K) newKey, newValue);
            return (KeyValue<K, V>) KeyValue.pair(newKey, newValue);
        }
        return super.setMergedKeyValue(key, newValue);
    }
}
