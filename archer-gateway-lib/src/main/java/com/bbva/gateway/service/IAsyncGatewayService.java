package com.bbva.gateway.service;

import com.bbva.common.consumers.CRecord;

public interface IAsyncGatewayService<T> extends IGatewayService<T> {

    void createListener(CRecord output, T response);
}
