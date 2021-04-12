package com.bbva.gateway.util;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.gateway.config.annotations.ServiceConfig;
import com.bbva.gateway.service.AsyncGatewayService;
import com.bbva.gateway.service.base.GatewayBaseService;

@ServiceConfig(file = "command-service.yml")
public class CommandBaseService extends GatewayBaseService implements AsyncGatewayService {

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

    @Override
    public void createListener(final CRecord output, final Object response) {

    }
}
