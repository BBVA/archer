package com.bbva.gateway.util;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.gateway.config.annotations.ServiceConfig;
import com.bbva.gateway.service.GatewayService;
import com.bbva.gateway.service.base.GatewayBaseService;

@ServiceConfig(file = "event-service.yml")
public class EventBaseService extends GatewayBaseService implements GatewayService {

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
