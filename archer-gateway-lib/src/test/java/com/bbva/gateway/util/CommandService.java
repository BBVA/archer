package com.bbva.gateway.util;

import com.bbva.common.consumers.CRecord;
import com.bbva.gateway.config.annotations.ServiceConfig;
import com.bbva.gateway.service.IGatewayService;
import com.bbva.gateway.service.impl.GatewayService;

@ServiceConfig(file = "command-service.yml")
public class CommandService extends GatewayService implements IGatewayService {

    @Override
    public Object call(final CRecord record) {
        return null;
    }

    @Override
    protected Boolean isSuccess(final Object response) {
        return null;
    }

    @Override
    public void postInitActions() {

    }

    @Override
    public void processResult(final CRecord originRecord, final Object result) {

    }
}
