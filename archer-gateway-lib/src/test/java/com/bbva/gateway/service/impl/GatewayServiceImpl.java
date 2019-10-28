package com.bbva.gateway.service.impl;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.gateway.service.impl.beans.Person;


public class GatewayServiceImpl extends GatewayService<Person> {

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
    public void postInitActions() {

    }

    @Override
    public void processResult(final CRecord originRecord, final Person result) {
        sendEvent(originRecord, null);
    }
}
