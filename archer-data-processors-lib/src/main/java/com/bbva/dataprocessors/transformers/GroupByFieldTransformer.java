package com.bbva.dataprocessors.transformers;

import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import com.bbva.dataprocessors.records.GenericRecordList;
import com.bbva.dataprocessors.util.ObjectUtils;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Transformer for grpoup by a field in a entity state
 *
 * @param <K> Key class
 * @param <V> Value class
 */
public class GroupByFieldTransformer<K, V extends SpecificRecord> implements Transformer<K, V, KeyValue<K, GenericRecord>> {

    private static final Logger logger = LoggerFactory.getLogger(GroupByFieldTransformer.class);
    private static final String uuidFieldName = "uuid";

    protected ProcessorContext context;
    private KeyValueStore<K, GenericRecord> stateStore;
    private final String stateStoreName;
    private final GenericRecordList<V> genericRecordList;

    /**
     * Constructor
     *
     * @param stateStoreName state store
     * @param valueClass     Class type of value
     */
    public GroupByFieldTransformer(final String stateStoreName, final Class<V> valueClass) {
        this.stateStoreName = stateStoreName;
        genericRecordList = new GenericRecordList<>(valueClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore<K, GenericRecord>) this.context.getStateStore(stateStoreName);
    }

    /**
     * Add/Update the new record to the list grouping by the pivot field
     *
     * @param newKey   record key
     * @param newValue record
     * @return new key/value pair
     */
    @Override
    public KeyValue<K, GenericRecord> transform(final K newKey, final V newValue) {
        K resultKey = null;
        GenericRecord resultValue = null;

        try {
            if (newValue == null) {
                final RecordHeaders headers = new RecordHeaders(context.headers());
                logger.info("****** HEADERS: {}", headers);
                final String headerUuid = headers.find(ChangelogHeaderType.UUID_KEY).asString();

                final KeyValueIterator<K, GenericRecord> iterator = stateStore.all();
                final AtomicBoolean found = new AtomicBoolean(false);
                KeyValue<K, GenericRecord> record;

                while (iterator.hasNext() && !found.get()) {
                    record = iterator.next();
                    final List<V> resultValueList = genericRecordList.getList(record.value);

                    if (!resultValueList.isEmpty()) {
                        final Method resultValueListItemIdMethod = resultValueList
                                .get(0)
                                .getClass()
                                .getMethod(ObjectUtils.getFieldNameMethod(uuidFieldName, true));
                        String uuid;
                        int i = 0;
                        while (i < resultValueList.size() && !found.get()) {
                            uuid = (String) resultValueListItemIdMethod.invoke(resultValueList.get(i));
                            if (uuid.equals(headerUuid)) {
                                resultValueList.remove(i);
                                resultKey = record.key;
                                resultValue = genericRecordList.getRecord(resultValueList);
                                found.set(true);
                                break;
                            }
                            i++;
                        }
                    }
                }

            } else {
                GenericRecord storedValue = stateStore.get(newKey);
                final List<V> resultList;

                if (storedValue == null) {
                    resultList = new ArrayList<>();
                    resultList.add(newValue);
                    storedValue = genericRecordList.getRecord(resultList);

                } else {
                    resultList = genericRecordList.getList(storedValue);
                    final AtomicBoolean found = new AtomicBoolean(false);

                    if (!resultList.isEmpty()) {
                        final Method resultListItemIdMethod = ((V) resultList
                                .get(0))
                                .getClass()
                                .getMethod(ObjectUtils.getFieldNameMethod(uuidFieldName, true));
                        final String newUuid = (String) resultListItemIdMethod.invoke(newValue);
                        String storedUuid;
                        int i = 0;
                        while (i < resultList.size() && !found.get()) {
                            storedUuid = (String) resultListItemIdMethod.invoke(resultList.get(i));
                            if (storedUuid.equals(newUuid)) {
                                resultList.remove(i);
                                resultList.add(i, newValue);
                                found.set(true);
                            }
                            i++;
                        }
                    }
                    if (!found.get()) {
                        resultList.add(newValue);
                    }
                    storedValue = genericRecordList.getRecord(resultList);
                }
                resultKey = newKey;
                resultValue = storedValue;

            }
        } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            logger.error(e.getMessage(), e);
        }

        KeyValue<K, GenericRecord> resultKeyValue = null;
        if (resultKey != null) {
            stateStore.put(resultKey, resultValue);
            resultKeyValue = KeyValue.pair(resultKey, resultValue);
        }

        return resultKeyValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        //Do nothing
    }

}
