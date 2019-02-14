package com.bbva.example.service;


import com.bbva.archer.avro.gateway.example.GatewayCommand;
import com.bbva.archer.avro.gateway.example.OutputEvent;
import com.bbva.common.consumers.CRecord;
import com.bbva.example.constants.Constants;
import com.bbva.example.util.ExampleUtil;
import com.bbva.gateway.config.annotations.ServiceConfig;
import com.bbva.gateway.service.impl.HttpGatewayService;
import okhttp3.ResponseBody;
import retrofit2.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

@ServiceConfig(file = "example/services/getService.yml")
public class GetService extends HttpGatewayService {

    @Override
    public void postInitActions() {
        super.postInitActions();
        new ExampleUtil<GatewayCommand>().generateEvents(config, new GatewayCommand(String.valueOf(new Date().getTime()), "mensaje de prueba"), 2000, Arrays.asList(Constants.TEST_SERVICES));
    }

    @Override
    public void processResult(final CRecord originRecord, final Response result) {
        try {
            sendEvent(originRecord, new OutputEvent("getService-" + ResponseBody.class.cast(result.body()).string()));
        } catch (final IOException e) {
            sendEvent(originRecord, new OutputEvent("getService-Error"));
        }
    }
}
