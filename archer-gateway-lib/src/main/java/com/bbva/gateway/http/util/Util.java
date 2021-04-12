package com.bbva.gateway.http.util;

import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.http.model.HttpRequest;

import java.util.Map;

/**
 * Http utilities
 */
public final class Util {

    private Util() {
        throw new UnsupportedOperationException();
    }

    /**
     * Translate record to http object
     *
     * @param body   value of the body
     * @param config gateway configuration
     * @return http object
     */
    public static HttpRequest composeHttpRequest(final Object body, final GatewayConfig config) {
        final HttpRequest request = new HttpRequest();
        request.setHeaders((Map<String, String>) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_HTTP_HEADERS));
        request.setMethod((String) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_HTTP_METHOD));
        request.setBody(body.toString());

        return request;
    }


}
