package com.bbva.gateway.service.impl;

import com.bbva.common.consumers.CRecord;
import com.bbva.gateway.bean.HttpBean;
import com.bbva.gateway.http.HttpRequest;
import com.bbva.gateway.http.RetrofitClient;
import com.bbva.gateway.service.IGatewayService;
import kst.logging.Logger;
import kst.logging.LoggerFactory;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
import org.codehaus.jackson.JsonParser;
import retrofit2.Response;
import retrofit2.Retrofit;

import java.io.IOException;
import java.util.Map;

import static com.bbva.gateway.constants.ConfigConstants.*;

public abstract class HttpGatewayService
        extends GatewayService<Response> implements IGatewayService<Response> {

    private static final Logger logger = LoggerFactory.getLogger(HttpGatewayService.class);
    private Retrofit retrofit;
    private Map<String, String> queryParams;

    @Override
    public void postInitActions() {
        retrofit = RetrofitClient.build((String) config.getGateway().get(GATEWAY_URI));
        queryParams = (Map<String, String>) config.getGateway().get(GATEWAY_QUERY_PARAMS);
        om.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    @Override
    public Response call(final CRecord record) {
        final HttpRequest httpObject = traslateRecordToHttp(record);
        return RetrofitClient.call(retrofit, httpObject, queryParams);
    }

    protected HttpRequest traslateRecordToHttp(final CRecord record) {
        final Map<String, Object> gatewayConfig = config.getGateway();
        final HttpRequest request = new HttpRequest();
        request.setHeaders((Map<String, String>) gatewayConfig.get(GATEWAY_HTTP_HEADERS));
        request.setMethod((String) gatewayConfig.get(GATEWAY_HTTP_METHOD));
        request.setBody(record.value().toString());

        return request;
    }

    @Override
    protected Boolean isSuccess(final Response response) {
        return response.isSuccessful();
    }

    @Override
    public String parseChangelogToString(final Response response) {

        try {
            final HttpBean responseChangelog = new HttpBean(response.code(),
                    response.body() != null ? ResponseBody.class.cast(response.body()).string() : "",
                    response.headers().toMultimap());
            return om.writeValueAsString(responseChangelog);
        } catch (final IOException e) {
            logger.error("Problem in the serialization", e);
            return null;
        }

    }

    @Override
    public Response parseChangelogFromString(final String output) {
        try {
            final HttpBean httpChangelog = om.readValue(output, HttpBean.class);
            final Response response = Response.success(httpChangelog.getCode(),
                    ResponseBody.create(MediaType.get("application/json"), httpChangelog.getBody()));
            response.headers().toMultimap().putAll(httpChangelog.getHeaders());
            return response;
        } catch (final IOException e) {
            logger.error("Cannot parse to string changelog", e);
            return null;
        }
    }
}
