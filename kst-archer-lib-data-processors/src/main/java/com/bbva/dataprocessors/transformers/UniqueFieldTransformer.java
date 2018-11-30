package com.bbva.dataprocessors.transformers;

import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class UniqueFieldTransformer<K, V extends SpecificRecordBase, K1> implements Transformer<K, V, KeyValue<K1, K>> {
    private static final LoggerGen logger = LoggerGenesis.getLogger(UniqueFieldTransformer.class.getName());
    private KeyValueStore<K1, K> stateStore;
    private String stateStoreName;
    private String fieldPath;

    public UniqueFieldTransformer(String stateStoreName, String fieldPath) {
        this.stateStoreName = stateStoreName;
        this.fieldPath = fieldPath;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        stateStore = (KeyValueStore<K1, K>) context.getStateStore(stateStoreName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public KeyValue<K1, K> transform(final K key, final V value) {
        K1 uniqueFieldKey = null;
        K uniqueFieldValue = null;
        if (value == null) {
            KeyValueIterator<K1, K> iterator = stateStore.all();
            while (iterator.hasNext()) {
                KeyValue<K1, K> row = iterator.next();
                if (row.value.equals(key)) {
                    uniqueFieldKey = row.key;
                    break;
                }
            }
        } else {
            String[] splitFieldPath = fieldPath.split("\\.");
            try {
                Object fieldValue = value;
                for (String field : splitFieldPath) {
                    if (fieldValue instanceof SpecificRecordBase) {
                        fieldValue = ((SpecificRecordBase) fieldValue).get(field);

                        if (fieldValue != null && !(fieldValue instanceof SpecificRecordBase)) {
                            uniqueFieldKey = (K1) fieldValue;
                            uniqueFieldValue = key;
                        }
                    }
                }
            } catch (NullPointerException e) {
                // ignored
            } catch (Exception e) {
                logger.error(e);
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