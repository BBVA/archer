package com.bbva.common.producers;

/**
 * Producer callback interface
 */
public interface ProducerCallback {
    /**
     * Method executed on produce record completed
     *
     * @param id        object id
     * @param exception exception in the production
     */
    void onCompletion(Object id, Exception exception);
}
