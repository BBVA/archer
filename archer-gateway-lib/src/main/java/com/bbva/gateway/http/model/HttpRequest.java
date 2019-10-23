package com.bbva.gateway.http.model;

import java.util.Map;

/**
 * Object to serialize http requests
 */
public class HttpRequest {
    private Object body;
    private Map<String, String> headers;
    private String method;

    public Object getBody() {
        return body;
    }

    public void setBody(final Object body) {
        this.body = body;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(final Map<String, String> headers) {
        this.headers = headers;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(final String method) {
        this.method = method;
    }

}
