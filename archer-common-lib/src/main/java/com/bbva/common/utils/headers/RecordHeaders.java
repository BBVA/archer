package com.bbva.common.utils.headers;

import com.bbva.common.utils.ByteArrayValue;
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

    public ByteArrayValue find(final HeaderType key) {
        return find(key.getName());
    }

    public ByteArrayValue find(final String key) {
        ByteArrayValue value = null;
        for (final Header header : headers) {
            if (header.key().equals(key)) {
                value = new ByteArrayValue(header.value(), true);
                break;
            }
        }
        return value;
    }

    public void add(final HeaderType key, final HeaderType value) {
        add(key, new ByteArrayValue(value.getName()));
    }

    public void add(final HeaderType key, final ByteArrayValue value) {
        add(key.getName(), value);
    }

    public void add(final String key, final ByteArrayValue value) {
        headers.add(new RecordHeader(key, value.getBytes()));
    }

    public void add(final RecordHeader recordHeader) {
        headers.add(recordHeader);
    }

    public void addAll(final RecordHeaders recordHeaders) {
        headers.addAll(recordHeaders.getList());
    }

    public List<Header> getList() {
        return headers;
    }

    @Override
    public String toString() {

        final StringBuilder value = new StringBuilder("[");
        for (final Header header : headers) {
            value.append("{key: ").append(header.key()).append(", value: ").append(ByteArrayValue.Serde.deserializeAs(header.value()).toString()).append("}");
        }
        value.append("]");
        return value.toString();
    }
}
