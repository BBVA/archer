package com.bbva.gateway.service;

import com.bbva.common.consumers.record.CRecord;

/**
 * Asynchronous gateway service
 *
 * @param <T> Result type
 */
public interface AsyncGatewayService<T> extends GatewayService<T> {

    /**
     * Listener to manage callback
     *
     * @param output   record
     * @param response result
     */
    void createListener(CRecord output, T response);
}
