package com.bbva.gateway.transformers;


import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.dataprocessors.transformers.EntityTransformer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.KeyValue;

/**
 * Transformer for gateway changelog
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class HeaderAsKeyStateTransformer<K, V> extends EntityTransformer<K, V> {

    /**
     * Constructor
     *
     * @param stateStoreName state store
     */
    public HeaderAsKeyStateTransformer(final String stateStoreName) {
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
        final Header header = context.headers().lastHeader(CommandHeaderType.UUID_KEY.getName());
        if (header != null) {
            final String newKey = new ByteArrayValue(header.value(), true).asString();
            stateStore.put((K) newKey, newValue);
            return (KeyValue<K, V>) KeyValue.pair(newKey, newValue);
        }
        return super.setMergedKeyValue(key, newValue);
    }
}
