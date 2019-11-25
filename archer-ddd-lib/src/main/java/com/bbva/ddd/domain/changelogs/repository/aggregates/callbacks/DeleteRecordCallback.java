package com.bbva.ddd.domain.changelogs.repository.aggregates.callbacks;

import com.bbva.common.producers.callback.ProducerCallback;

/**
 * Aggregate callback interface to manage delete record
 */
public interface DeleteRecordCallback {

    /**
     * Method to apply the deletion
     *
     * @param method   Method Invoked
     * @param callback To manage apply response
     */
    void apply(String method, ProducerCallback callback);
}
