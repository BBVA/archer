package com.bbva.ddd.domain.changelogs.repository;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.producers.Producer;
import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.common.producers.record.PRecord;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.ddd.domain.changelogs.producers.ChangelogRecordMetadata;
import com.bbva.ddd.domain.changelogs.repository.aggregates.AbstractAggregate;
import com.bbva.ddd.domain.changelogs.repository.aggregates.AggregateBase;
import com.bbva.ddd.domain.changelogs.repository.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.changelogs.repository.aggregates.annotations.AggregateParent;
import com.bbva.ddd.domain.commands.consumers.CommandRecord;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Class  to interact with the aggregates
 *
 * @param <V> Value class type
 */
public final class RepositoryImpl<V extends SpecificRecordBase> implements Repository {

    private static final Logger logger = LoggerFactory.getLogger(RepositoryImpl.class);

    private final Producer producer;
    private final String aggregateUUID;
    private final RepositoryCache<V> repositoryCache;

    private String parentChangelogName;
    private Field expectedParentField;
    private Class<? extends SpecificRecordBase> parentValueClass;

    private final CRecord referenceRecord;
    private final Boolean isReplay;

    /**
     * Repository implementation constructor
     *
     * @param referenceRecord record consumed
     * @param isReplay        flag of replay
     */
    public RepositoryImpl(final CRecord referenceRecord, final Boolean isReplay) {
        this(referenceRecord, new DefaultProducer(ConfigBuilder.get()), isReplay);
    }

    /**
     * Repository implementation constructor
     *
     * @param referenceRecord record consumed
     * @param producer        producer instance
     * @param isReplay        flag of replay
     */
    public RepositoryImpl(final CRecord referenceRecord, final Producer producer, final Boolean isReplay) {
        aggregateUUID = UUID.randomUUID().toString();
        this.producer = producer;
        repositoryCache = new RepositoryCache<>();
        this.referenceRecord = referenceRecord;
        this.isReplay = isReplay;
    }

    @Override
    public <T extends AggregateBase> T create(final Class<T> aggregateClass, final SpecificRecordBase value, final ProducerCallback callback) {
        return create(aggregateClass, ((CommandRecord) referenceRecord).entityUuid(), value, callback);
    }

    @Override
    public <T extends AggregateBase> T create(final Class<T> aggregateClass, final String aggregateKey, final SpecificRecordBase value, final ProducerCallback callback) {
        final Aggregate aggregate = aggregateClass.getAnnotation(Aggregate.class);
        if (aggregate == null) {
            throw new ApplicationException("Aggregate class not have specific annotation");
        }
        setDependencies(aggregateClass);
        try {
            value.put("uuid", aggregateKey);
        } catch (final NullPointerException e) {
            logger.warn("Schema has not field uuid");
        }

        final AggregateBase aggregateBaseInstance = getAggregateInstance(aggregate.baseName(), aggregateClass, aggregateKey, value);

        logger.debug("Creating PRecord of type {}", value.getClass().getName());

        save(aggregate.baseName(), aggregateClass, (String) aggregateBaseInstance.getId(), (V) aggregateBaseInstance.getData(), referenceRecord, "constructor",
                callback);

        return (T) aggregateBaseInstance;
    }

    @Override
    public <T extends AggregateBase> T load(final Class<T> aggregateClass, final String key) {
        final Aggregate aggregate = aggregateClass.getAnnotation(Aggregate.class);
        if (aggregate == null) {
            throw new ApplicationException("Aggregate class not have specific annotation");
        }
        setDependencies(aggregateClass);
        logger.debug("Loading from store {}", aggregate.baseName());

        final V value = repositoryCache.getCurrentState(aggregate.baseName(), key);

        if (value != null) {
            logger.debug("Value found for id {}", key);
            return loadAggregateInstance(aggregate.baseName(), key, value, aggregateClass);
        }
        return null;
    }

    private <T extends AggregateBase> T loadAggregateInstance(final String baseName, final String key, final V value, final Class<? extends AggregateBase> aggregateClass) {
        final AggregateBase aggregateBaseInstance;
        try {
            aggregateBaseInstance = aggregateClass.getConstructor(key.getClass(), value.getClass())
                    .newInstance(key, value);
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            logger.error("Not constructor found for aggregate class", e);
            throw new ApplicationException("Not constructor found for aggregate class", e);
        }

        logger.info("Aggregate loaded");

        aggregateBaseInstance.setApplyRecordCallback(
                (method, newValue, referenceRecord, callback) ->
                        save(baseName, aggregateClass, (String) aggregateBaseInstance.getId(), (V) newValue, referenceRecord, method, callback));

        if (aggregateBaseInstance instanceof AbstractAggregate) {
            ((AbstractAggregate) aggregateBaseInstance).setDeleteRecordCallback(
                    (method, referenceRecord, callback) ->
                            delete(baseName, (String) aggregateBaseInstance.getId(), headers(aggregateClass, method, referenceRecord), callback));
        }

        logger.debug("Returning Aggregate instance");
        return (T) aggregateBaseInstance;
    }

    private void setDependencies(final Class<? extends AggregateBase> aggregateClass) {

        if (aggregateClass.isAnnotationPresent(AggregateParent.class)) {

            final Class<? extends SpecificRecordBase> childValueClass = getValueClass(aggregateClass);

            final Class<? extends AbstractAggregate> parentClass = aggregateClass
                    .getAnnotation(AggregateParent.class).value();
            parentValueClass = getValueClass(parentClass);

            final Field[] parentFields = parentValueClass.getDeclaredFields();
            for (final Field parentField : parentFields) {
                if (Modifier.isPublic(parentField.getModifiers())
                        && childValueClass.isAssignableFrom(parentField.getType())) {
                    parentChangelogName = parentClass.getAnnotation(Aggregate.class).baseName()
                            + AppConfig.CHANGELOG_RECORD_NAME_SUFFIX;
                    expectedParentField = parentField;
                    break;
                }
            }
            if (expectedParentField == null) {
                throw new ApplicationException("Something rare happened: Parent has not field for child");
            }
        }
    }

    private static Class getValueClass(final Class annotatedClass) {
        return (Class) ((ParameterizedType) annotatedClass.getGenericSuperclass()).getActualTypeArguments()[1];
    }

    private ChangelogRecordMetadata save(final String baseName, final Class<? extends AggregateBase> aggregateClass, final String key, final V value, final CRecord referenceRecord, final String method,
                                         final ProducerCallback callback) {
        ChangelogRecordMetadata changelogRecordMetadata = null;
        final String changelogName = baseName + AppConfig.CHANGELOG_RECORD_NAME_SUFFIX;
        if (parentValueClass != null && expectedParentField != null) {
            try {
                final SpecificRecordBase parentValue = parentValueClass.getConstructor().newInstance();
                expectedParentField.set(parentValue, value);
                changelogRecordMetadata = propagate(parentChangelogName, key, parentValue, headers(aggregateClass, method, referenceRecord),
                        (id, e) -> {
                            if (e != null) {
                                logger.error("Error saving the object", e);
                            } else {
                                logger.info("Parent updated");
                                propagate(changelogName, key, value, headers(aggregateClass, method, referenceRecord), callback);
                            }
                        });
            } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                logger.error("Problems saving the object", e);
            }
        } else {
            changelogRecordMetadata = propagate(changelogName, key, value, headers(aggregateClass, method, referenceRecord), callback);
        }

        repositoryCache.updateState(key, value, changelogRecordMetadata);

        return changelogRecordMetadata;
    }

    private ChangelogRecordMetadata propagate(final String changelogName, final String key,
                                              final SpecificRecordBase value, final RecordHeaders headers, final ProducerCallback callback) {
        logger.info("Propagate PRecord for key: {}", key);
        ChangelogRecordMetadata changelogMessageMetadata = null;
        final PRecord record = new PRecord(changelogName, key, value, headers);

        try {
            final Future<RecordMetadata> result = producer.send(record, callback);
            changelogMessageMetadata = new ChangelogRecordMetadata(result.get());

        } catch (final InterruptedException | ExecutionException e) {
            logger.error("Problems propagating the object", e);
        }

        return changelogMessageMetadata;
    }

    private ChangelogRecordMetadata delete(final String baseName, final String key, final RecordHeaders headers,
                                           final ProducerCallback callback) {

        logger.info("Delete PRecord for key: {}", key);
        final String changelogName = baseName + AppConfig.CHANGELOG_RECORD_NAME_SUFFIX;
        ChangelogRecordMetadata changelogRecordMetadata = null;
        final PRecord record = new PRecord(changelogName, key, null, headers);

        try {
            final Future<RecordMetadata> result = producer.send(record, callback);
            changelogRecordMetadata = new ChangelogRecordMetadata(result.get());

        } catch (final InterruptedException | ExecutionException e) {
            logger.error("Problems deleting the object", e);
        }

        repositoryCache.updateState(key, null, changelogRecordMetadata);

        return changelogRecordMetadata;
    }

    private <T extends AggregateBase> AggregateBase getAggregateInstance(final String baseName, final Class<T> aggregateClass, final String key, final SpecificRecordBase value) {
        final AggregateBase aggregateBaseInstance;
        try {
            aggregateBaseInstance = aggregateClass.getConstructor(key.getClass(), value.getClass())
                    .newInstance(key, value);
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            logger.error("Not constructor found for aggregate class", e);
            throw new ApplicationException("Not constructor found for aggregate class", e);
        }

        logger.info("Aggregate loaded");

        aggregateBaseInstance.setApplyRecordCallback(
                (method, newValue, referenceRecord, callback) ->
                        save(baseName, aggregateClass, (String) aggregateBaseInstance.getId(), (V) newValue, referenceRecord, method, callback));

        if (aggregateBaseInstance instanceof AbstractAggregate) {
            ((AbstractAggregate) aggregateBaseInstance).setDeleteRecordCallback(
                    (method, referenceRecord, callback) ->
                            delete(baseName, (String) aggregateBaseInstance.getId(), headers(aggregateClass, method, referenceRecord), callback));
        }

        logger.debug("Returning Aggregate instance");
        return aggregateBaseInstance;
    }

    private RecordHeaders headers(final Class aggregateClass, final String aggregateMethod, final CRecord referenceRecord) {

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.TYPE_KEY, ChangelogHeaderType.TYPE_VALUE);

        if (referenceRecord != null) {
            recordHeaders.add(ChangelogHeaderType.UUID_KEY, referenceRecord.recordHeaders().find(CommandHeaderType.ENTITY_UUID_KEY).asString());
            recordHeaders.add(CommonHeaderType.REFERENCE_RECORD_KEY_KEY, referenceRecord.key());
            recordHeaders.add(CommonHeaderType.REFERENCE_RECORD_TYPE_KEY,
                    referenceRecord.recordHeaders().find(CommonHeaderType.TYPE_KEY).asString());
            recordHeaders.add(CommonHeaderType.REFERENCE_RECORD_POSITION_KEY,
                    referenceRecord.topic() + "-" + referenceRecord.partition() + "-" + referenceRecord.offset());
        }

        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY,
                (referenceRecord != null && referenceRecord.isReplayMode()) || isReplay);
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_UUID_KEY, aggregateUUID);
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_NAME_KEY, aggregateClass.getName());
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_METHOD_KEY, aggregateMethod);

        logger.debug("CRecord getList: {}", recordHeaders.toString());

        return recordHeaders;
    }

}
