package com.bbva.gateway.service.impl;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.http.RetrofitClient;
import com.bbva.gateway.http.model.HttpRequest;
import com.bbva.gateway.http.util.Util;
import com.bbva.gateway.service.IAsyncGatewayService;
import org.codehaus.jackson.JsonParser;
import retrofit2.Retrofit;

import java.util.Map;

/**
 * Http asynchronous gateway implementation
 *
 * @param <T> Response type
 */
public abstract class HttpAsyncGatewayService<T>
        extends AsyncGatewayService<T> implements IAsyncGatewayService<T> {

    private Retrofit retrofit;
    private Map<String, String> queryParams;

    /**
     * {@inheritDoc}
     */
    @Override
    public void postInitActions() {
        retrofit = RetrofitClient.build((String) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_URI));
        queryParams = (Map<String, String>) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_QUERY_PARAMS);
        om.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T call(final CRecord record) {
        final HttpRequest httpObject = Util.translateRecordToHttp(record, config);
        return (T) RetrofitClient.call(retrofit, httpObject, queryParams);
    }


}
