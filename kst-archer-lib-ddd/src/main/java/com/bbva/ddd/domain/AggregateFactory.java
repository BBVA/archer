package com.bbva.ddd.domain;

import com.bbva.common.producers.ProducerCallback;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.avro.specific.SpecificRecordBase;

import java.lang.reflect.InvocationTargetException;

public class AggregateFactory {

    public static <V extends SpecificRecordBase, T extends AggregateBase> T create(Class<T> aggregateClass, V record,
            CommandRecord commandMessage, ProducerCallback callback) {
        // String key;
        // boolean isReplayMode;
        //
        // if (trigger.getMode().equalsIgnoreCase("replay")) {
        // key = (trigger instanceof CommandRecord) ?
        // ((CommandRecord) trigger).entityId() :
        // ApplicationServices
        // .get()
        // .getStore(ApplicationConfig.GLOBAL_NAME_PREFIX + "reference-key-map")
        // .findById(trigger.uuid()).toString();
        //
        // isReplayMode = true;
        // } else {
        // key = (trigger instanceof CommandRecord) ? ((CommandRecord) trigger).entityId() :
        // UUID.randomUUID().toString();
        // isReplayMode = false;
        // }
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
            } else {

            }

        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
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
                } else {

                }

            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

}
