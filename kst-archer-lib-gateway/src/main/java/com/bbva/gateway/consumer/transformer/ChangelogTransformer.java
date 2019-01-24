package com.bbva.gateway.consumer.transformer;


import com.bbva.common.utils.ByteArrayValue;
import com.bbva.dataprocessors.transformers.EntityTransformer;
import com.bbva.ddd.domain.changelogs.read.ChangelogRecord;
import org.apache.kafka.streams.KeyValue;

public class ChangelogTransformer<K, V> extends EntityTransformer<K, V> {

    public ChangelogTransformer(final String stateStoreName) {
        super(stateStoreName);
    }

    @Override
    protected KeyValue<K, V> setMergedKeyValue(final K key, final V newValue) {
        if (this.context.headers().lastHeader(ChangelogRecord.TRIGGER_REFERENCE_KEY) != null) {
            final String newKey = new ByteArrayValue(this.context.headers().lastHeader(ChangelogRecord.TRIGGER_REFERENCE_KEY).value()).asString();
            this.stateStore.put((K) newKey, newValue);
            return (KeyValue<K, V>) KeyValue.pair(newKey, newValue);
        }
        return super.setMergedKeyValue(key, newValue);
    }
}
