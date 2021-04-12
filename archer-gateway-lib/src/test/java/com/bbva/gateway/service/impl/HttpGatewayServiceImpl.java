package com.bbva.gateway.service.impl;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.ddd.domain.handlers.contexts.HandlerContext;
import com.bbva.gateway.service.http.HttpGatewayService;
import retrofit2.Response;


public class HttpGatewayServiceImpl extends HttpGatewayService<Object> {

    @Override
    public Object body(final CRecord consumedRecord) {
        return null;
    }

    @Override
    public void processResponse(final HandlerContext context, final Response result) {
        context
                .event("httpGateway")
                .producerName("httpGatewayService")
                .value(null)
                .to("event_output")
                .build()
                .send((key, e) -> {

                });
    }

}
