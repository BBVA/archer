package com.bbva.ddd.domain;

import com.bbva.common.producers.ProducerCallback;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;

public class AggregateFactory {

    private static final Logger logger = Logger.getLogger(Domain.class);

    public static <V extends SpecificRecordBase, T extends AggregateBase> T create(Class<T> aggregateClass, V record,
            CommandRecord commandMessage, ProducerCallback callback) {

        return create(aggregateClass, null, record, commandMessage, callback);
    }

    @SuppressWarnings("unchecked")
    public static <K, V extends SpecificRecordBase, T extends AggregateBase> T create(Class<T> aggregateClass,
            String key, V value, CommandRecord commandMessage, ProducerCallback callback) {
        Aggregate aggregateAnnotation = aggregateClass.getAnnotation(Aggregate.class);
        String basename = aggregateAnnotation.baseName();

        final Repository repository = Repositories.get(basename);
        AggregateBase aggregateBaseInstance = null;

        try {
            if (repository != null) {
                aggregateBaseInstance = repository.create(key, value, commandMessage, callback);
            }

        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
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
    public static <T extends AggregateBase> T load(Class<T> aggregateClass, String id) {
        if (aggregateClass.isAnnotationPresent(Aggregate.class)) {

            Aggregate aggregateAnnotation = aggregateClass.getAnnotation(Aggregate.class);
            String basename = aggregateAnnotation.baseName();

            final Repository repository = Repositories.get(basename);

            try {
                if (repository != null) {
                    return (T) repository.loadFromStore(id);
                }

            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                logger.error("Problems found in load factory", e);
            }
        }

        return null;
    }

}
