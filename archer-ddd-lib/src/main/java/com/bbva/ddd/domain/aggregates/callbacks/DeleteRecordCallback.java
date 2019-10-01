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
     * @param method          Method Invoked
     * @param valueClass      Class type of the value
     * @param referenceRecord Reference record that triggers the event
     * @param callback        To manage apply response
     */
    void apply(String method, Class<V> valueClass, CRecord referenceRecord, ProducerCallback callback);
}
