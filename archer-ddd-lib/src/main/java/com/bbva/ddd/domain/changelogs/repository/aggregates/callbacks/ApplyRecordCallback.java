package com.bbva.ddd.domain.changelogs.repository.aggregates.callbacks;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.callback.ProducerCallback;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Aggregate callback interface to manage apply record responses
 */
public interface ApplyRecordCallback {

    /**
     * Method to apply the record
     *
     * @param method          of the action
     * @param value           record specification
     * @param referenceRecord reference record that triggers the event
     * @param callback        to manage apply response
     */
    void apply(String method, SpecificRecordBase value, CRecord referenceRecord, ProducerCallback callback);
}
