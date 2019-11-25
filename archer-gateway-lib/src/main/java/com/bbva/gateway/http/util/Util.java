package com.bbva.gateway.http.util;

import com.bbva.common.consumers.record.CRecord;
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
     * @param record record
     * @param config gateway configuration
     * @return http object
     */
    public static HttpRequest translateRecordToHttp(final CRecord record, final GatewayConfig config) {
        final HttpRequest request = new HttpRequest();
        request.setHeaders((Map<String, String>) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_HTTP_HEADERS));
        request.setMethod((String) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_HTTP_METHOD));
        request.setBody(record.value().toString());

        return request;
    }


}
