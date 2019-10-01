package com.bbva.ddd.domain.aggregates.callbacks;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.ProducerCallback;
import org.apache.avro.specific.SpecificRecord;

/**
 * Aggregate callback interface to manage apply record responses
 */
public interface ApplyRecordCallback {

    /**
     * Method to apply the record
     *
     * @param method   of the action
     * @param record   record specification
     * @param message  new record with metadata
     * @param callback to manage apply response
     */
    void apply(String method, SpecificRecord record, CRecord message, ProducerCallback callback);
}
