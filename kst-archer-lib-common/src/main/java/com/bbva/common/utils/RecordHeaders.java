package com.bbva.common.utils;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RecordHeaders {

    protected List<Header> headers = new ArrayList<>();

    public RecordHeaders() {
    }

    public RecordHeaders(Headers headers) {
        this.headers = Arrays.asList(headers.toArray());
    }

    public RecordHeaders(List<Header> headers) {
        this.headers = headers;
    }

    public GenericValue find(String key) {
        GenericValue value = null;
        for (Header header : headers) {
            if (header.key().equals(key)) {
                value = new GenericValue(header.value());
                break;
            }
        }
        return value;
    }

    public void add(String key, GenericValue value) {
        headers.add(new RecordHeader(key, value));
    }

    public void add(RecordHeader recordHeader) {
        headers.add(recordHeader);
    }

    public void addAll(RecordHeaders recordHeaders) {
        this.headers.addAll(recordHeaders.getList());
    }

    public List<Header> getList() {
        return headers;
    }

    @Override
    public String toString() {
        String value = "[";
        for (Header header : headers) {
            value += "{key: " + header.key() + ", value: " + new GenericValue(header.value()).asString() + "}";
        }
        value += "]";
        return value;
    }
}
