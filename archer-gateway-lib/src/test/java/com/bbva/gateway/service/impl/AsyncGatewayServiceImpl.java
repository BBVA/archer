package com.bbva.gateway.service.impl;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.gateway.service.base.AsyncGatewayBaseService;
import com.bbva.gateway.service.impl.beans.Person;


public class AsyncGatewayServiceImpl extends AsyncGatewayBaseService<Person> {


    @Override
    public Person call(final CRecord record) {
        return new Person("name");
    }

    @Override
    protected boolean isSuccess(final Person response) {
        return true;
    }

    @Override
    public void init() {

    }

    @Override
    public String transactionId(final Person response) {
        return null;
    }

    @Override
    public void createListener(final CRecord output, final Person response) {

    }
}
