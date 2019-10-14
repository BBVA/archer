package com.bbva.ddd.domain.changelogs.repository.aggregates.callbacks;

/**
 * Aggregate callback interface to manage aggregate responses
 */
public interface AggregateCallback {

    /**
     * Method to handle aggregate repsonse
     */
    void onComplete();

}

