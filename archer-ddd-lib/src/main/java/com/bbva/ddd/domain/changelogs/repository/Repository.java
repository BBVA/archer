package com.bbva.ddd.domain.changelogs.repository;

import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.ddd.domain.changelogs.repository.aggregates.AggregateBase;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Class  to interact with the aggregates
 */
public interface Repository {

    <T extends AggregateBase> T create(Class<T> aggregateClass, SpecificRecordBase value, ProducerCallback callback);

    <T extends AggregateBase> T create(Class<T> aggregateClass, String key, SpecificRecordBase value, ProducerCallback callback);

    <T extends AggregateBase> T load(Class<T> aggregateClass, String key);
}
