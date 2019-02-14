package com.bbva.ddd.domain.changelogs;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.PRecord;
import com.bbva.common.producers.ProducerCallback;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.RecordHeaders;
import com.bbva.dataprocessors.exceptions.StoreNotFoundException;
import com.bbva.ddd.HelperDomain;
import com.bbva.ddd.domain.aggregates.AbstractAggregateBase;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.aggregates.annotations.AggregateParent;
import com.bbva.ddd.domain.aggregates.exceptions.AggregateDependenciesException;
import com.bbva.ddd.domain.changelogs.read.ChangelogRecord;
import com.bbva.ddd.domain.changelogs.write.ChangelogRecordMetadata;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.util.StoreUtil;
import kst.logging.Logger;
import kst.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class Repository<K, V extends SpecificRecordBase> {

    private static final Logger logger = LoggerFactory.getLogger(Repository.class);

    private final Class<? extends AggregateBase> aggregateClass;
    private final CachedProducer producer;
    private final String changelogName;
    private final String baseName;
    private final String aggregateUUID;

    private String parentChangelogName;
    private Field expectedParentField;
    private Class<? extends SpecificRecordBase> parentValueClass;

    public Repository(final String baseName, final Class<? extends AggregateBase> aggregateClass,
                      final ApplicationConfig applicationConfig) throws AggregateDependenciesException {
        aggregateUUID = UUID.randomUUID().toString();
        this.aggregateClass = aggregateClass;
        producer = new CachedProducer(applicationConfig);
        this.baseName = baseName;
        changelogName = baseName + ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX;

        setDependencies();

        logger.info("Repository for {} initialized", aggregateClass.getName());
    }

    public String getBaseName() {
        return baseName;
    }

    @SuppressWarnings("unchecked")
    public AggregateBase create(String key, final V value, final CommandRecord commandMessage,
                                final ProducerCallback callback) {
        key = (key == null) ? commandMessage.entityId() : key;

        try {
            value.put("uuid", key);
        } catch (final NullPointerException e) {
            logger.warn("Schema has not field uuid");
        }

        final AggregateBase aggregateBaseInstance = getAggregateIntance(key, value);

        logger.debug("Creating PRecord of type {}", value.getClass().getName());

        save((String) aggregateBaseInstance.getId(), (V) aggregateBaseInstance.getData(), commandMessage, "constructor",
                callback);

        return aggregateBaseInstance;
    }

    public AggregateBase loadFromStore(final String key) {
        logger.debug("Loading from store {}", baseName);

        final V value;

        try {
            value = StoreUtil.<String, V>getStore(baseName).findById(key);
        } catch (final StoreNotFoundException e) {
            return null;
        }

        if (value != null) {
            logger.debug("Value found for id {}", key);
            return getAggregateIntance(key, value);
        }
        return null;
    }

    private void setDependencies() throws AggregateDependenciesException {

        if (aggregateClass.isAnnotationPresent(AggregateParent.class)) {

            final Class<? extends SpecificRecordBase> childValueClass = getValueClass(aggregateClass);

            final Class<? extends AbstractAggregateBase> parentClass = aggregateClass
                    .getAnnotation(AggregateParent.class).value();
            parentValueClass = getValueClass(parentClass);

            final Field[] parentFields = parentValueClass.getDeclaredFields();
            for (final Field parentField : parentFields) {
                if (Modifier.isPublic(parentField.getModifiers())
                        && childValueClass.isAssignableFrom(parentField.getType())) {
                    parentChangelogName = parentClass.getAnnotation(Aggregate.class).baseName()
                            + ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX;
                    expectedParentField = parentField;
                    break;
                }
            }
            if (expectedParentField == null) {
                throw new AggregateDependenciesException("Something rare happened: Parent has not field for child");
            }
        }
    }

    private static Class getValueClass(final Class anotatedClass) {
        return (Class) ((ParameterizedType) anotatedClass.getGenericSuperclass()).getActualTypeArguments()[1];
    }

    private ChangelogRecordMetadata save(final String key, final V value, final CRecord record, final String method,
                                         final ProducerCallback callback) {
        ChangelogRecordMetadata changelogMessageMetadata = null;

        if (parentValueClass != null && expectedParentField != null) {
            try {
                final SpecificRecordBase parentValue = parentValueClass.getConstructor().newInstance();
                expectedParentField.set(parentValue, value);
                changelogMessageMetadata = propagate(parentChangelogName, key, parentValue, headers(method, record),
                        (id, e) -> {
                            if (e != null) {
                                logger.error("Error saving the object", e);
                            } else {
                                logger.info("Parent updated");
                                propagate(changelogName, key, value, headers(method, record), callback);
                            }
                        });
            } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                logger.error("Problems saving the object", e);
            }
        } else {
            changelogMessageMetadata = propagate(changelogName, key, value, headers(method, record), callback);
        }
        return changelogMessageMetadata;
    }

    private ChangelogRecordMetadata propagate(final String changelogName, final String key,
                                              final SpecificRecordBase value, final RecordHeaders headers, final ProducerCallback callback) {
        logger.info("Propagate PRecord for key: {}", key);
        ChangelogRecordMetadata changelogMessageMetadata = null;
        final PRecord<String, SpecificRecordBase> record = new PRecord<>(changelogName, key, value, headers);

        try {
            final Future<RecordMetadata> result = producer.add(record, callback);
            changelogMessageMetadata = new ChangelogRecordMetadata(result.get());

        } catch (final InterruptedException | ExecutionException e) {
            logger.error("Problems propagating the object", e);
        }

        return changelogMessageMetadata;
    }

    private ChangelogRecordMetadata delete(final K key, final Class<V> valueClass, final RecordHeaders headers,
                                           final ProducerCallback callback) {

        logger.info("Delete PRecord for key: {}", key);

        ChangelogRecordMetadata changelogMessageMetadata = null;
        final PRecord<K, V> record = new PRecord<>(changelogName, key, null, headers);

        try {
            final Future<RecordMetadata> result = producer.remove(record, valueClass, callback);
            changelogMessageMetadata = new ChangelogRecordMetadata(result.get());

        } catch (final InterruptedException | ExecutionException e) {
            logger.error("Problems deleting the object", e);
        }

        return changelogMessageMetadata;
    }

    private AggregateBase getAggregateIntance(final String key, final V value) {
        final AggregateBase aggregateBaseInstance;
        try {
            aggregateBaseInstance = aggregateClass.getConstructor(key.getClass(), value.getClass())
                    .newInstance(key, value);
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            logger.error("Not constructor found for aggregate class", e);
            throw new ApplicationException("Not constructor found for aggregate class");
        }

        logger.info("Aggregate loaded");

        aggregateBaseInstance.setApplyRecordCallback((method, record, message,
                                                      callback) -> save((String) aggregateBaseInstance.getId(), (V) record, message, method, callback));

        if (aggregateBaseInstance instanceof AbstractAggregateBase) {
            ((AbstractAggregateBase) aggregateBaseInstance).setDeleteRecordCallback(
                    (method, recordClass, message, callback) -> delete((K) aggregateBaseInstance.getId(), recordClass,
                            headers(method, message), callback));
        }

        logger.debug("Returning Aggregate instance");
        return aggregateBaseInstance;
    }

    private RecordHeaders headers(final String aggregateMethod, final CRecord record) {

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CRecord.TYPE_KEY, new ByteArrayValue(ChangelogRecord.TYPE_CHANGELOG_VALUE));
        recordHeaders.add(ChangelogRecord.UUID_KEY, new ByteArrayValue(UUID.randomUUID().toString()));
        if (record != null && record.key() != null) {
            recordHeaders.add(ChangelogRecord.TRIGGER_REFERENCE_KEY,
                    new ByteArrayValue(record.key()));
        }
        recordHeaders.add(CRecord.FLAG_REPLAY_KEY, new ByteArrayValue(
                (record != null && record.isReplayMode()) || HelperDomain.get().isReplayMode()));
        recordHeaders.add(ChangelogRecord.AGGREGATE_UUID_KEY, new ByteArrayValue(aggregateUUID));
        recordHeaders.add(ChangelogRecord.AGGREGATE_NAME_KEY, new ByteArrayValue(this.aggregateClass.getName()));
        recordHeaders.add(ChangelogRecord.AGGREGATE_METHOD_KEY, new ByteArrayValue(aggregateMethod));

        logger.debug("CRecord getList: {}", recordHeaders.toString());

        return recordHeaders;
    }
}
