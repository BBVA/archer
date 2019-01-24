package com.bbva.gateway.service;

import com.bbva.common.consumers.CRecord;
import com.bbva.gateway.config.Configuration;

public interface IGatewayService<T> {
    void init(Configuration configuration, String baseName);

    void postInitActions();

    void processRecord(CRecord record);

    void processResult(CRecord originRecord, T result);
}
