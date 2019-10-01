package com.bbva.ddd.domain.aggregates.callbacks;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.ProducerCallback;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Aggregate callback interface to manage delete record
 */
public interface DeleteRecordCallback<K, V extends SpecificRecordBase> {

    /**
     * Method to apply the deletion
     *
     * @param method      invoked
     * @param recordClass class type of record
     * @param record      record to delete
     * @param callback    to manage apply response
     */
    void apply(String method, Class<V> recordClass, CRecord record, ProducerCallback callback);
}
