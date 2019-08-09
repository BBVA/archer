package com.bbva.gateway.service.impl;

import com.bbva.common.consumers.CRecord;
import retrofit2.Response;


public class HttpGatewayServiceImpl extends HttpGatewayService {


    @Override
    public void processResult(CRecord originRecord, Response result) {
        sendEvent(null, null);
    }

}
