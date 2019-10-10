package com.bbva.gateway.service.impl;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.gateway.http.HttpRequest;
import com.bbva.gateway.http.RetrofitClient;
import com.bbva.gateway.service.IAsyncGatewayService;
import org.codehaus.jackson.JsonParser;
import retrofit2.Retrofit;

import java.util.Map;

import static com.bbva.gateway.constants.ConfigConstants.*;

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
        retrofit = RetrofitClient.build((String) config.getGateway().get(GATEWAY_URI));
        queryParams = (Map<String, String>) config.getGateway().get(GATEWAY_QUERY_PARAMS);
        om.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T call(final CRecord record) {
        final HttpRequest httpObject = traslateRecordToHttp(record);
        return (T) RetrofitClient.call(retrofit, httpObject, queryParams);
    }

    /**
     * Translate a record to http object
     *
     * @param record record
     * @return http object
     */
    protected HttpRequest traslateRecordToHttp(final CRecord record) {
        final Map<String, Object> gatewayConfig = config.getGateway();
        final HttpRequest request = new HttpRequest();
        request.setHeaders((Map<String, String>) gatewayConfig.get(GATEWAY_HTTP_HEADERS));
        request.setMethod((String) gatewayConfig.get(GATEWAY_HTTP_METHOD));
        request.setBody(record.value().toString());

        return request;
    }

}
