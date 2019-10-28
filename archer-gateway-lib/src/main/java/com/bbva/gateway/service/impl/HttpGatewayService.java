package com.bbva.gateway.service.impl;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.http.RetrofitClient;
import com.bbva.gateway.http.model.HttpBean;
import com.bbva.gateway.http.model.HttpRequest;
import com.bbva.gateway.http.util.Util;
import com.bbva.gateway.service.IGatewayService;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
import org.codehaus.jackson.JsonParser;
import retrofit2.Response;
import retrofit2.Retrofit;

import java.io.IOException;
import java.util.Map;

/**
 * Http gateway service implementation
 */
public abstract class HttpGatewayService
        extends GatewayService<Response> implements IGatewayService<Response> {

    private static final Logger logger = LoggerFactory.getLogger(HttpGatewayService.class);
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
    public Response call(final CRecord record) {
        final HttpRequest httpObject = Util.translateRecordToHttp(record, config);
        return RetrofitClient.call(retrofit, httpObject, queryParams);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isSuccess(final Response response) {
        return response.isSuccessful();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String parseChangelogToString(final Response response) {

        try {
            final HttpBean responseChangelog = new HttpBean(response.code(),
                    response.body() != null ? ((ResponseBody) response.body()).string() : "",
                    response.headers().toMultimap());
            return om.writeValueAsString(responseChangelog);
        } catch (final IOException e) {
            logger.error("Problem in the serialization", e);
            return null;
        }

    }

    /**
     * {@inheritDoc}
     */
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
