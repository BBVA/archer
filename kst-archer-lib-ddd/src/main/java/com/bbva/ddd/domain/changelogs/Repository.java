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
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
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

    private static final LoggerGen logger = LoggerGenesis.getLogger(Repository.class.getName());

    private final Class<? extends AggregateBase> aggregateClass;
    private final CachedProducer producer;
    private final String changelogName;
    private final String baseName;
    private final String aggregateUUID;

    private String parentChangelogName;
    private Field expectedParentField;
    private Class<? extends SpecificRecordBase> parentValueClass = null;

    public Repository(final String baseName, final Class<? extends AggregateBase> aggregateClass,
                      final ApplicationConfig applicationConfig) throws AggregateDependenciesException {
        aggregateUUID = UUID.randomUUID().toString();
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

    public AggregateBase create(String key, final V value, final CommandRecord commandMessage,
                                final ProducerCallback callback)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        key = (key == null) ? commandMessage.entityId() : key;

        try {
            value.put("uuid", key);
        } catch (final NullPointerException e) {
            logger.warn("Schema has not field uuid");
        }

        final AggregateBase aggregateBaseInstance = getAggregateIntance(key, value);

        logger.debug("Creating PRecord of type " + value.getClass().getName());

        save((String) aggregateBaseInstance.getId(), (V) aggregateBaseInstance.getData(), commandMessage, "constructor",
                callback);

        return aggregateBaseInstance;
    }

    public AggregateBase loadFromStore(final String key)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        logger.debug("Loading from store " + baseName);

        V value;

        try {
            value = ApplicationServices.<String, V>getStore(baseName).findById(key);

        } catch (final NullPointerException e) {
            value = null;
        }

        if (value != null) {
            logger.debug("Value found for id " + key);

            return getAggregateIntance(key, value);

        } else {
            return null;
        }
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
                final String errorMessage = "Something rare happened: Parent has not field for child";
                logger.error(errorMessage);
                throw new AggregateDependenciesException(errorMessage);
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
                                logger.error(e);
                            } else {
                                logger.info("Parent updated");
                                propagate(changelogName, key, value, headers(method, record), callback);
                            }
                        });
            } catch (final InstantiationException | IllegalAccessException | InvocationTargetException
                    | NoSuchMethodException e) {
                logger.error("Problems saving the object", e);
            }
        } else {
            changelogMessageMetadata = propagate(changelogName, key, value, headers(method, record), callback);
        }
        return changelogMessageMetadata;
    }

    private ChangelogRecordMetadata propagate(final String changelogName, final String key,
                                              final SpecificRecordBase value, final RecordHeaders headers, final ProducerCallback callback) {
        logger.info("Propagate PRecord for key: " + key);
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
                                           final ProducerCallback callback)
            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        logger.info("Delete PRecord for key: " + key);
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

    private AggregateBase getAggregateIntance(final String key, final V value)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        final AggregateBase aggregateBaseInstance = aggregateClass.getConstructor(key.getClass(), value.getClass())
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

    private RecordHeaders headers(final String aggregateMethod, final CRecord record) {

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CRecord.TYPE_KEY, new GenericValue(ChangelogRecord.TYPE_CHANGELOG_VALUE));
        recordHeaders.add(ChangelogRecord.UUID_KEY, new GenericValue(UUID.randomUUID().toString()));
        recordHeaders.add(ChangelogRecord.TRIGGER_REFERENCE_KEY,
                new GenericValue(record != null ? record.key() : ""));
        recordHeaders.add(CRecord.FLAG_REPLAY_KEY, new GenericValue(
                (record != null && record.isReplayMode()) || ApplicationServices.get().isReplayMode()));
        recordHeaders.add(ChangelogRecord.AGGREGATE_UUID_KEY, new GenericValue(aggregateUUID));
        recordHeaders.add(ChangelogRecord.AGGREGATE_NAME_KEY, new GenericValue(this.aggregateClass.getName()));
        recordHeaders.add(ChangelogRecord.AGGREGATE_METHOD_KEY, new GenericValue(aggregateMethod));

        logger.debug("CRecord getList: " + recordHeaders.toString());

        return recordHeaders;
    }
}