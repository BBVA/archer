package com.bbva.common.consumers.contexts;

import com.bbva.common.consumers.record.CRecord;

/**
 * Consumer context to manage the actions before consume a record
 */
public interface ConsumerContext {

    /**
     * Return the consumer in context record
     *
     * @return the record
     */
    CRecord consumedRecord();

}
