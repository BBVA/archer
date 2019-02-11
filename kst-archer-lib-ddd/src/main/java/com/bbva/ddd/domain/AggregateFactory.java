package com.bbva.ddd.domain;

import com.bbva.common.producers.ProducerCallback;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import kst.logging.Logger;
import kst.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;

import java.lang.reflect.InvocationTargetException;

public class AggregateFactory {

    private static final Logger logger = LoggerFactory.getLogger(AggregateFactory.class);

    public static <V extends SpecificRecordBase, T extends AggregateBase> T create(final Class<T> aggregateClass, final V record,
                                                                                   final CommandRecord commandMessage, final ProducerCallback callback) {

        return create(aggregateClass, null, record, commandMessage, callback);
    }

    @SuppressWarnings("unchecked")
    public static <K, V extends SpecificRecordBase, T extends AggregateBase> T create(final Class<T> aggregateClass,
                                                                                      final String key, final V value, final CommandRecord commandMessage, final ProducerCallback callback) {

        final Repository repository = getRepositoryFromAggregate(aggregateClass);

        AggregateBase aggregateBaseInstance = null;

        try {
            if (repository != null) {
                aggregateBaseInstance = repository.create(key, value, commandMessage, callback);
            }

        } catch (final NoSuchMethodException | IllegalAccessException | InstantiationException
                | InvocationTargetException e) {
            logger.error("Problems found in create factory", e);
        }

        return aggregateClass.cast(aggregateBaseInstance);
    }


    /**
     * @param aggregateClass
     * @param id
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T extends AggregateBase> T load(final Class<T> aggregateClass, final String id) {
        if (aggregateClass.isAnnotationPresent(Aggregate.class)) {

            final Repository repository = getRepositoryFromAggregate(aggregateClass);

            try {
                if (repository != null) {
                    return (T) repository.loadFromStore(id);
                }

            } catch (final NoSuchMethodException | IllegalAccessException | InstantiationException
                    | InvocationTargetException e) {
                logger.error("Problems found in load factory", e);
            }
        }

        return null;
    }

    private static <T extends AggregateBase> Repository getRepositoryFromAggregate(final Class<T> aggregateClass) {
        final Aggregate aggregateAnnotation = aggregateClass.getAnnotation(Aggregate.class);
        final String basename = aggregateAnnotation.baseName();

        return Repositories.get(basename);
    }
}
