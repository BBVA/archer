package com.bbva.gateway.service;

import com.bbva.common.consumers.CRecord;

/**
 * Asynchronous gateway service
 *
 * @param <T> Result type
 */
public interface IAsyncGatewayService<T> extends IGatewayService<T> {

    /**
     * Listener to manage callback
     *
     * @param output   record
     * @param response result
     */
    void createListener(CRecord output, T response);
}
