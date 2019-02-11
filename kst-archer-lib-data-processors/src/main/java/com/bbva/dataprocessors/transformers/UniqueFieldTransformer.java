package com.bbva.dataprocessors.transformers;

import kst.logging.Logger;
import kst.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class UniqueFieldTransformer<K, V extends SpecificRecordBase, K1> implements Transformer<K, V, KeyValue<K1, K>> {
    private static final Logger logger = LoggerFactory.getLogger(UniqueFieldTransformer.class);
    private KeyValueStore<K1, K> stateStore;
    private final String stateStoreName;
    private final String fieldPath;

    public UniqueFieldTransformer(final String stateStoreName, final String fieldPath) {
        this.stateStoreName = stateStoreName;
        this.fieldPath = fieldPath;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        stateStore = (KeyValueStore<K1, K>) context.getStateStore(stateStoreName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public KeyValue<K1, K> transform(final K key, final V value) {
        K1 uniqueFieldKey = null;
        K uniqueFieldValue = null;
        if (value == null) {
            final KeyValueIterator<K1, K> iterator = stateStore.all();
            while (iterator.hasNext()) {
                final KeyValue<K1, K> row = iterator.next();
                if (row.value.equals(key)) {
                    uniqueFieldKey = row.key;
                    break;
                }
            }
        } else {
            final String[] splitFieldPath = fieldPath.split("\\.");
            try {
                Object fieldValue = value;
                for (final String field : splitFieldPath) {
                    if (fieldValue instanceof SpecificRecordBase) {
                        fieldValue = ((SpecificRecordBase) fieldValue).get(field);

                        if (fieldValue != null && !(fieldValue instanceof SpecificRecordBase)) {
                            uniqueFieldKey = (K1) fieldValue;
                            uniqueFieldValue = key;
                        }
                    }
                }
            } catch (final Exception e) {
                logger.error("Error transforming the data", e);
            }
        }

        if (uniqueFieldKey == null) {
            return null;
        } else {
            stateStore.put(uniqueFieldKey, uniqueFieldValue);
            return KeyValue.pair(uniqueFieldKey, uniqueFieldValue);
        }
    }

    @Override
    public void close() {
    }
}
