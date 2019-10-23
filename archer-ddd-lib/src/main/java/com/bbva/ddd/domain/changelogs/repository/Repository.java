package com.bbva.ddd.domain.changelogs.repository;

import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.ddd.domain.changelogs.repository.aggregates.AggregateBase;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Class to interact with the aggregates
 */
public interface Repository {

    /**
     * Create a aggregate without specific key
     *
     * @param aggregateClass class of aggregate
     * @param value          value data
     * @param callback       callabak to manage the response
     * @param <T>            aggregate class type
     * @return aggregate instance
     */
    <T extends AggregateBase> T create(Class<T> aggregateClass, SpecificRecordBase value, ProducerCallback callback);

    /**
     * Create a aggregate with specific key
     *
     * @param aggregateClass class of aggregate
     * @param key            key of aggregate
     * @param value          value data
     * @param callback       callabak to manage the response
     * @param <T>            aggregate class type
     * @return aggregate instance
     */
    <T extends AggregateBase> T create(Class<T> aggregateClass, String key, SpecificRecordBase value, ProducerCallback callback);

    /**
     * Load and aggregate instance
     *
     * @param aggregateClass aggregate class
     * @param key            key to load
     * @param <T>            aggregate class type
     * @return aggregate instance
     */
    <T extends AggregateBase> T load(Class<T> aggregateClass, String key);
}
