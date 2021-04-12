package com.bbva.gateway.service.http;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.http.RetrofitClient;
import com.bbva.gateway.http.model.HttpBean;
import com.bbva.gateway.http.model.HttpRequest;
import com.bbva.gateway.service.GatewayService;
import com.bbva.gateway.service.base.GatewayBaseService;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
import org.codehaus.jackson.JsonParser;
import retrofit2.Response;
import retrofit2.Retrofit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Http gateway service implementation
 */
public abstract class HttpGatewayService<T>
        extends GatewayBaseService<Response> implements GatewayService<Response> {

    private static final Logger logger = LoggerFactory.getLogger(HttpGatewayService.class);
    private Retrofit retrofit;
    private Map<String, String> queryParams;

    /**
     * Method that acts as adapter of the body value
     *
     * @param consumedRecord Record consumed from event store
     * @return T which is an instance of the type of the body
     */
    public abstract T body(final CRecord consumedRecord);

    public String url(final CRecord consumedRecord) {
        return (String) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_URI);
    }

    public Map<String, String> headers(final CRecord consumedRecord) {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        return headers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init() {
        om.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Response call(final CRecord record) {
        final HttpRequest request = new HttpRequest();
        request.setHeaders(headers(record));
        request.setMethod((String) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_HTTP_METHOD));
        request.setBody(body(record).toString());
        return RetrofitClient.call(RetrofitClient.build(url(record)), request, queryParams);
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
    protected String parseChangelogToString(final Response response) {

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
    protected Response parseChangelogFromString(final String output) {
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
