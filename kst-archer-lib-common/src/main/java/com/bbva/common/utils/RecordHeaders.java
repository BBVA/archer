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

    public ByteArrayValue find(String key) {
        ByteArrayValue value = null;
        for (Header header : headers) {
            if (header.key().equals(key)) {
                value = new ByteArrayValue(header.value());
                break;
            }
        }
        return value;
    }

    public void add(String key, ByteArrayValue value) {
        headers.add(new RecordHeader(key, value.getBytes()));
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
        StringBuilder value = new StringBuilder("[");
        for (Header header : headers) {
            value.append("{key: ").append(header.key()).append(", value: ").append(ByteArrayValue.Serde.deserializeAs(String.class, header.value())).append("}");
        }
        value.append("]");
        return value.toString();
    }
}
