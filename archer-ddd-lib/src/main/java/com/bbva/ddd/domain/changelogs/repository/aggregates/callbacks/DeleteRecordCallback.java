package com.bbva.ddd.domain.changelogs.repository.aggregates.callbacks;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.callback.ProducerCallback;

/**
 * Aggregate callback interface to manage delete record
 */
public interface DeleteRecordCallback {

    /**
     * Method to apply the deletion
     *
     * @param method          Method Invoked
     * @param valueClass      Class type of the value
     * @param referenceRecord Reference record that triggers the event
     * @param callback        To manage apply response
     */
    void apply(String method, CRecord referenceRecord, ProducerCallback callback);
}
