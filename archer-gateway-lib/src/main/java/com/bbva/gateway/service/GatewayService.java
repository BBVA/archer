package com.bbva.gateway.service;

import com.bbva.ddd.domain.handlers.contexts.HandlerContext;
import com.bbva.gateway.config.GatewayConfig;

/**
 * Gateway service interface
 *
 * @param <R> Response type
 */
public interface GatewayService<R> {

    /**
     * Initialize the service
     *
     * @param configuration configuration
     */
    void initialize(GatewayConfig configuration);

//    /**
//     * Actions post initialization
//     */
//    void postInitActions();

    /**
     * Process record
     *
     * @param context record to process
     */
    void processRecord(HandlerContext context);

    /**
     * Process result
     *
     * @param context context
     * @param result  call result
     */
    void processResponse(HandlerContext context, R result);
}
