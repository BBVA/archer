package com.bbva.gateway.service;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.ddd.domain.consumers.HandlerContextImpl;
import com.bbva.gateway.config.Configuration;

/**
 * Gateway service interface
 *
 * @param <T> Response type
 */
public interface IGatewayService<T> {

    /**
     * Initialize the service
     *
     * @param configuration configuration
     * @param baseName      base name
     */
    void init(Configuration configuration, String baseName);

    /**
     * Actions post initialization
     */
    void postInitActions();

    /**
     * Process record
     *
     * @param record record to process
     */
    void processRecord(HandlerContextImpl record);

    /**
     * Process call result
     *
     * @param originRecord record
     * @param result       call result
     */
    void processResult(CRecord originRecord, T result);
}
