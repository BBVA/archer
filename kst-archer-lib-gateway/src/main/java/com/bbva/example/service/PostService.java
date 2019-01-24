package com.bbva.example.service;

import com.bbva.archer.avro.gateway.example.OutputEvent;
import com.bbva.common.consumers.CRecord;
import com.bbva.gateway.config.ServiceConfig;
import com.bbva.gateway.service.impl.HttpGatewayService;
import okhttp3.ResponseBody;
import retrofit2.Response;

@ServiceConfig(file = "example/services/postService.yml")
public class PostService extends HttpGatewayService {

    @Override
    public void postInitActions() {
        super.postInitActions();
    }

    @Override
    public void processResult(final CRecord originRecord, final Response result) {
        sendEvent(originRecord, new OutputEvent("postService-" + ResponseBody.class.cast(result.body())));
    }
}
