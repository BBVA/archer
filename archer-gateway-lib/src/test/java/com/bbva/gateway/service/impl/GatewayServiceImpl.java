package com.bbva.gateway.service.impl;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.ddd.domain.handlers.contexts.HandlerContext;
import com.bbva.gateway.service.base.GatewayBaseService;
import com.bbva.gateway.service.impl.beans.Person;


public class GatewayServiceImpl extends GatewayBaseService<Person> {

    public GatewayServiceImpl() {
        handleOutPutted("value", null);
        handleOutPutted("value", new Exception());
    }

    @Override
    public Person call(final CRecord record) {
        return new Person("result");
    }

    @Override
    protected boolean isSuccess(final Person response) {
        return true;
    }

    @Override
    public void init() {

    }

    @Override
    public void processResponse(final HandlerContext context, final Person result) {
        sendEvent(originRecord, null);
        context
                .event("")
                .value(context.consumedRecord().value())
                .to("")
                .build()
                .send((key, e) -> {

                });
    }
}
