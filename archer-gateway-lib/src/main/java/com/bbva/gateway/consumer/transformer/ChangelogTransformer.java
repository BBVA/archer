package com.bbva.gateway.consumer.transformer;


import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import com.bbva.dataprocessors.transformers.EntityTransformer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.KeyValue;

public class ChangelogTransformer<K, V> extends EntityTransformer<K, V> {

    public ChangelogTransformer(final String stateStoreName) {
        super(stateStoreName);
    }

    @Override
    protected KeyValue<K, V> setMergedKeyValue(final K key, final V newValue) {
        final Header header = this.context.headers().lastHeader(ChangelogHeaderType.UUID_KEY.getName());
        if (header != null) {
            final String newKey = new ByteArrayValue(header.value()).asString();
            this.stateStore.put((K) newKey, newValue);
            return (KeyValue<K, V>) KeyValue.pair(newKey, newValue);
        }
        return super.setMergedKeyValue(key, newValue);
    }
}
