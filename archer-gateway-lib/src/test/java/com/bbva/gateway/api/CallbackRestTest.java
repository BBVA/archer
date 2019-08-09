package com.bbva.gateway.api;

import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

import javax.ws.rs.core.Response;

@RunWith(JUnit5.class)
public class CallbackRestTest {

    @DisplayName("Create CallBack Ok")
    @Test
    public void createCallBackOk() {
        final CallbackRest callbackRest = new CallbackRest();
        CallbackRest.init();
        Assertions.assertNotNull(callbackRest);
    }

    @DisplayName("CallBack response Ok")
    @Test
    public void callBackResponseOk() {
        Assertions.assertEquals(Response.Status.OK.getStatusCode(), CallbackRest.callback("request").getStatus());
    }

}
