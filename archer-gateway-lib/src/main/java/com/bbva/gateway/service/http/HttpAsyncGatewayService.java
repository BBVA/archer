package com.bbva.gateway.service.http;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.http.RetrofitClient;
import com.bbva.gateway.http.model.HttpRequest;
import com.bbva.gateway.http.util.Util;
import com.bbva.gateway.service.AsyncGatewayService;
import com.bbva.gateway.service.base.AsyncGatewayBaseService;
import org.codehaus.jackson.JsonParser;
import retrofit2.Retrofit;

import java.util.Map;

/**
 * Http asynchronous gateway implementation
 *
 * @param <R> Response type
 * @param <T> Type of the body
 */
public abstract class HttpAsyncGatewayService<R, T>
        extends AsyncGatewayBaseService<R> implements AsyncGatewayService<R> {

    private Retrofit retrofit;
    private Map<String, String> queryParams;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init() {
        retrofit = RetrofitClient.build((String) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_URI));
        queryParams = (Map<String, String>) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_QUERY_PARAMS);
        om.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public R call(final CRecord record) {
        final T bodyValue = body(record);
        final HttpRequest httpObject = Util.composeHttpRequest(bodyValue, config);
        return (R) RetrofitClient.call(retrofit, httpObject, queryParams);
    }

    /**
     * Method that acts as adapter of the body value
     *
     * @param consumedRecord Record consumed from event store
     * @return T which is an instance of the type of the body
     */
    public abstract T body(final CRecord consumedRecord);

}
