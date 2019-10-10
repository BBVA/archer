package com.bbva.ddd.domain;

import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Factory to manage aggregates
 */
public class AggregateFactory {

    /**
     * Create an aggregate and store value in a changelog. One of the uses of commandRecord is to get the entity uuid
     * so ensure idempotence in replay mode.
     * <pre>{@code
     *  try {
     *      final FooAggregate fooAggregate = AggregateFactory.create(FooAggregate.class, new Foo(), command, (id, e) -> {
     *          if (e != null) {
     *              e.printStackTrace();
     *          } else {
     *              logger.info("Foo created! id: " + id);
     *          }
     *      });
     *      final Foo foo = fooAggregate.getData();
     *  } catch (final NullPointerException e) {
     *      logger.warning("[WARN] Aggregate not found");
     *  }
     * }</pre>
     *
     * @param aggregateClass Class of the aggregate
     * @param value          Initial value to aggregate
     * @param commandRecord  Command record which has triggered the domain logic
     * @param callback       Callback executed when message is stored in changelog
     * @param <V>            Type of the value
     * @param <T>            Type of the resulting aggregate instance
     * @return An instance of the aggregateClass
     */
    public static <V extends SpecificRecordBase, T extends AggregateBase> T create(
            final Class<T> aggregateClass, final V value,
            final CommandRecord commandRecord, final ProducerCallback callback) {

        return create(aggregateClass, null, value, commandRecord, callback);
    }

    /**
     * Create an aggregate and store value in a changelog. In this case, the key is provided as an argument of the method.
     * <pre>{@code
     *  try {
     *      final String key = UUID.randomUUID().toString();
     *      final FooAggregate fooAggregate = AggregateFactory
     *          .create(FooAggregate.class, key, new Foo(), command, (id, e) -> {
     *              if (e != null) {
     *                  e.printStackTrace();
     *              } else {
     *                  logger.info("Foo created! id: " + id);
     *              }
     *          });
     *      final Foo foo = fooAggregate.getData();
     *  } catch (final NullPointerException e) {
     *      logger.warning("[WARN] Aggregate not found");
     *  }
     * }</pre>
     *
     * @param aggregateClass Class of the aggregate
     * @param key            Identifier of the entity
     * @param value          Initial value to aggregate
     * @param commandRecord  Command record which has triggered the domain logic
     * @param callback       Callback executed when message is stored in changelog
     * @param <V>            Type of the value
     * @param <T>            Type of the resulting aggregate instance
     * @return An instance of the aggregateClass
     */
    public static <V extends SpecificRecordBase, T extends AggregateBase> T create(
            final Class<T> aggregateClass,
            final String key, final V value, final CommandRecord commandRecord, final ProducerCallback callback) {

        final Repository repository = getRepositoryFromAggregate(aggregateClass);

        AggregateBase aggregateBaseInstance = null;

        if (repository != null) {
            aggregateBaseInstance = repository.create(key, value, commandRecord, callback);
        }


        return aggregateClass.cast(aggregateBaseInstance);
    }

    /**
     * Load an aggregate by key with the current state of the changelog.
     *
     * @param aggregateClass Class of the aggregate
     * @param key            Identifier of the entity
     * @param <T>            Type of the resulting aggregate instance
     * @return An aggregate instance
     */
    public static <T extends AggregateBase> T load(final Class<T> aggregateClass, final String key) {
        if (aggregateClass.isAnnotationPresent(Aggregate.class)) {

            final Repository repository = getRepositoryFromAggregate(aggregateClass);

            if (repository != null) {
                return (T) repository.loadFromStore(key);
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
