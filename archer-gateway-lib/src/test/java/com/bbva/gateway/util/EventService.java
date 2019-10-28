package com.bbva.gateway.util;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.gateway.config.annotations.ServiceConfig;
import com.bbva.gateway.service.IGatewayService;
import com.bbva.gateway.service.impl.GatewayService;

@ServiceConfig(file = "event-service.yml")
public class EventService extends GatewayService implements IGatewayService {

    @Override
    public Object call(final CRecord record) {
        return null;
    }

    @Override
    protected boolean isSuccess(final Object response) {
        return true;
    }

    @Override
    public void postInitActions() {

    }

    @Override
    public void processResult(final CRecord originRecord, final Object result) {

    }
}
