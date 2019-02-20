package com.bbva.ddd.domain;

import com.bbva.common.producers.ProducerCallback;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.avro.specific.SpecificRecordBase;

public class AggregateFactory {

    public static <V extends SpecificRecordBase, T extends AggregateBase> T create(
            final Class<T> aggregateClass, final V record,
            final CommandRecord commandMessage, final ProducerCallback callback) {

        return create(aggregateClass, null, record, commandMessage, callback);
    }

    public static <K, V extends SpecificRecordBase, T extends AggregateBase> T create(
            final Class<T> aggregateClass,
            final String key, final V value, final CommandRecord commandMessage, final ProducerCallback callback) {

        final Repository repository = getRepositoryFromAggregate(aggregateClass);

        AggregateBase aggregateBaseInstance = null;

        if (repository != null) {
            aggregateBaseInstance = repository.create(key, value, commandMessage, callback);
        }


        return aggregateClass.cast(aggregateBaseInstance);
    }

    public static <T extends AggregateBase> T load(final Class<T> aggregateClass, final String id) {
        if (aggregateClass.isAnnotationPresent(Aggregate.class)) {

            final Repository repository = getRepositoryFromAggregate(aggregateClass);

            if (repository != null) {
                return (T) repository.loadFromStore(id);
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
