package com.bbva.gateway.service.impl;

import com.bbva.common.consumers.record.CRecord;
import retrofit2.Response;


public class HttpGatewayServiceImpl extends HttpGatewayService {


    @Override
    public void processResult(final CRecord originRecord, final Response result) {
        sendEvent(null, null);
    }

}
