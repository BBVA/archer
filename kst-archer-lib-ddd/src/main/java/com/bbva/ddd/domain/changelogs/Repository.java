package com.bbva.ddd.domain.changelogs;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.PRecord;
import com.bbva.common.producers.ProducerCallback;
import com.bbva.common.utils.GenericValue;
import com.bbva.common.utils.RecordHeaders;
import com.bbva.ddd.ApplicationServices;
import com.bbva.ddd.domain.aggregates.AbstractAggregateBase;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.aggregates.annotations.AggregateParent;
import com.bbva.ddd.domain.aggregates.exceptions.AggregateDependenciesException;
import com.bbva.ddd.domain.changelogs.read.ChangelogRecord;
import com.bbva.ddd.domain.changelogs.write.ChangelogRecordMetadata;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class Repository<K, V extends SpecificRecordBase> {

    private final Class<? extends AggregateBase> aggregateClass;
    private final CachedProducer producer;
    private final String changelogName;
    private final String baseName;
    private final String aggregateUUID;
    private final Logger logger;

    private String parentChangelogName = null;
    private Field expectedParentField = null;
    private Class<? extends SpecificRecordBase> parentValueClass = null;

    public Repository(String baseName, Class<? extends AggregateBase> aggregateClass,
            ApplicationConfig applicationConfig) throws AggregateDependenciesException, InvocationTargetException,
            NoSuchMethodException, InstantiationException, IllegalAccessException {
        aggregateUUID = UUID.randomUUID().toString();
        logger = Logger.getLogger(Repository.class);
        this.aggregateClass = aggregateClass;
        producer = new CachedProducer(applicationConfig);
        this.baseName = baseName;
        changelogName = baseName + ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX;

        setDependencies();

        logger.info("Repository for " + aggregateClass.getName() + " initialized");
    }

    public String getBaseName() {
        return baseName;
    }

    @SuppressWarnings("unchecked")
    public AggregateBase create(String key, V value, CommandRecord commandMessage, ProducerCallback callback)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        key = (key == null) ? commandMessage.entityId() : key;

        try {
            value.put("uuid", key);
        } catch (NullPointerException e) {
            logger.warn("Schema has not field uuid");
        }

        final AggregateBase aggregateBaseInstance = getAggregateIntance(key, value);

        logger.debug("Creating PRecord of type " + value.getClass().getName());

        save((String) aggregateBaseInstance.getId(), (V) aggregateBaseInstance.getData(), commandMessage, "constructor",
                callback);

        return aggregateBaseInstance;
    }

    @SuppressWarnings("unchecked")
    public AggregateBase loadFromStore(String key) throws NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException, InterruptedException {
        logger.debug("Loading from store " + baseName);

        V value;

        try {
            value = ApplicationServices.get().<String, V> getStore(baseName).findById(key);

        } catch (NullPointerException e) {
            value = null;
        }

        if (value != null) {
            logger.debug("Value found for id " + key);

            return getAggregateIntance(key, value);

        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private void setDependencies() throws AggregateDependenciesException {

        if (aggregateClass.isAnnotationPresent(AggregateParent.class)) {

            Class<? extends SpecificRecordBase> childValueClass = getValueClass(aggregateClass);

            Class<? extends AbstractAggregateBase> parentClass = aggregateClass.getAnnotation(AggregateParent.class)
                    .value();
            parentValueClass = getValueClass(parentClass);

            Field[] parentFields = parentValueClass.getDeclaredFields();
            for (Field parentField : parentFields) {
                if (Modifier.isPublic(parentField.getModifiers())
                        && childValueClass.isAssignableFrom(parentField.getType())) {
                    parentChangelogName = parentClass.getAnnotation(Aggregate.class).baseName()
                            + ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX;
                    expectedParentField = parentField;
                    break;
                }
            }
            if (expectedParentField == null) {
                String errorMessage = "Something rare happened: Parent has not field for child";
                logger.error(errorMessage);
                throw new AggregateDependenciesException(errorMessage);
            }
        }
    }

    private Class getValueClass(Class anotatedClass) {
        return (Class) ((ParameterizedType) anotatedClass.getGenericSuperclass()).getActualTypeArguments()[1];
    }

    @SuppressWarnings("unchecked")
    private ChangelogRecordMetadata save(String key, V value, CRecord record, String method,
            ProducerCallback callback) {
        ChangelogRecordMetadata changelogMessageMetadata = null;

        if (parentValueClass != null && expectedParentField != null) {
            try {
                SpecificRecordBase parentValue = parentValueClass.getConstructor().newInstance();
                expectedParentField.set(parentValue, value);
                changelogMessageMetadata = propagate(parentChangelogName, key, parentValue, headers(method, record),
                        (id, e) -> {
                            if (e != null) {
                                e.printStackTrace();
                            } else {
                                logger.info("Parent updated");
                                propagate(changelogName, key, value, headers(method, record), callback);
                            }
                        });
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        } else {
            changelogMessageMetadata = propagate(changelogName, key, value, headers(method, record), callback);
        }
        return changelogMessageMetadata;
    }

    private ChangelogRecordMetadata propagate(String changelogName, String key, SpecificRecordBase value,
            RecordHeaders headers, ProducerCallback callback) {
        logger.info("Propagate PRecord for key: " + key);
        ChangelogRecordMetadata changelogMessageMetadata = null;
        PRecord<String, SpecificRecordBase> record = new PRecord<>(changelogName, key, value, headers);

        try {
            Future<RecordMetadata> result = producer.add(record, callback);
            changelogMessageMetadata = new ChangelogRecordMetadata(result.get());

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return changelogMessageMetadata;
    }

    private ChangelogRecordMetadata delete(K key, Class<V> valueClass, RecordHeaders headers, ProducerCallback callback)
            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        logger.info("Delete PRecord for key: " + key);
        ChangelogRecordMetadata changelogMessageMetadata = null;
        PRecord<K, V> record = new PRecord<>(changelogName, key, null, headers);

        try {
            Future<RecordMetadata> result = producer.remove(record, valueClass, callback);
            changelogMessageMetadata = new ChangelogRecordMetadata(result.get());

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return changelogMessageMetadata;
    }

    @SuppressWarnings("unchecked")
    private AggregateBase getAggregateIntance(String key, V value)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        AggregateBase aggregateBaseInstance = aggregateClass.getConstructor(key.getClass(), value.getClass())
                .newInstance(key, value);

        logger.info("Aggregate loaded");

        aggregateBaseInstance.setApplyRecordCallback((method, record, message,
                callback) -> save((String) aggregateBaseInstance.getId(), (V) record, message, method, callback));

        if (AbstractAggregateBase.class.isInstance(aggregateBaseInstance)) {
            ((AbstractAggregateBase) aggregateBaseInstance).setDeleteRecordCallback(
                    (method, recordClass, message, callback) -> delete((K) aggregateBaseInstance.getId(), recordClass,
                            headers(method, message), callback));
        }

        logger.debug("Returning Aggregate instance");

        return aggregateBaseInstance;
    }

    private RecordHeaders headers(String aggregateMethod, CRecord record) {

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CRecord.TYPE_KEY, new GenericValue(ChangelogRecord.TYPE_CHANGELOG_VALUE));
        recordHeaders.add(ChangelogRecord.UUID_KEY, new GenericValue(UUID.randomUUID().toString()));
        recordHeaders.add(ChangelogRecord.TRIGGER_REFERENCE_KEY,
                new GenericValue(record != null ? (String) record.key() : ""));
        recordHeaders.add(CRecord.FLAG_REPLAY_KEY, new GenericValue(
                (record != null && record.isReplayMode()) || ApplicationServices.get().isReplayMode()));
        recordHeaders.add(ChangelogRecord.AGGREGATE_UUID_KEY, new GenericValue(aggregateUUID));
        recordHeaders.add(ChangelogRecord.AGGREGATE_NAME_KEY, new GenericValue(this.aggregateClass.getName()));
        recordHeaders.add(ChangelogRecord.AGGREGATE_METHOD_KEY, new GenericValue(aggregateMethod));

        logger.debug("CRecord getList: " + recordHeaders.toString());

        return recordHeaders;
    }
}
