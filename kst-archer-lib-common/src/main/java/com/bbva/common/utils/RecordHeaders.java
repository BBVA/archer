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

    public RecordHeaders(final Headers headers) {
        this.headers = Arrays.asList(headers.toArray());
    }

    public RecordHeaders(final List<Header> headers) {
        this.headers = headers;
    }

    public ByteArrayValue find(final String key) {
        ByteArrayValue value = null;
        for (final Header header : headers) {
            if (header.key().equals(key)) {
                value = new ByteArrayValue(header.value());
                break;
            }
        }
        return value;
    }

    public void add(final String key, final ByteArrayValue value) {
        headers.add(new RecordHeader(key, value.getBytes()));
    }

    public void add(final RecordHeader recordHeader) {
        headers.add(recordHeader);
    }

    public void addAll(final RecordHeaders recordHeaders) {
        this.headers.addAll(recordHeaders.getList());
    }

    public List<Header> getList() {
        return headers;
    }

    @Override
    public String toString() {
        final StringBuilder value = new StringBuilder("[");
        String valueHeader;

        for (final Header header : headers) {
            try {
                valueHeader = ByteArrayValue.Serde.deserializeAs(String.class, header.value());
            } catch (final Exception e) {
                valueHeader = "";
            }
            value.append("{key: ").append(header.key()).append(", value: ").append(valueHeader).append("}");
        }
        value.append("]");
        return value.toString();
    }
}
