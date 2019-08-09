package com.bbva.gateway.service.impl;

import com.bbva.common.consumers.CRecord;
import com.bbva.gateway.service.impl.beans.Person;


public class AsyncGatewayServiceImpl extends AsyncGatewayService<Person> {


    @Override
    public Person call(final CRecord record) {
        return new Person("name");
    }

    @Override
    protected Boolean isSuccess(final Person response) {
        return true;
    }

    @Override
    public void postInitActions() {

    }

    @Override
    public String getId(final Person response) {
        return null;
    }

    @Override
    public void createListener(final CRecord output, final Person response) {

    }
}
