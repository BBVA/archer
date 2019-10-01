package com.bbva.common.utils.headers;

import com.bbva.common.utils.ByteArrayValue;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Record headers
 */
public class RecordHeaders {

    protected List<Header> headers = new ArrayList<>();

    /**
     * Constructor
     */
    public RecordHeaders() {
    }

    /**
     * Constructor
     *
     * @param headers list of headers
     */
    public RecordHeaders(final Headers headers) {
        this.headers = Arrays.asList(headers.toArray());
    }

    /**
     * Constructor
     *
     * @param headers list of headers
     */
    public RecordHeaders(final List<Header> headers) {
        this.headers = headers;
    }

    /**
     * Find header by header type
     *
     * @param key header type with name of the key
     * @return header value
     */
    public ByteArrayValue find(final HeaderType key) {
        return find(key.getName());
    }

    /**
     * Find header by key
     *
     * @param key header key
     * @return header value
     */
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

    /**
     * Add header with header types
     *
     * @param key   key
     * @param value Any object as value
     */
    public void add(final HeaderType key, final Object value) {
        add(key, new ByteArrayValue(value));
    }

    /**
     * Add header with header types
     *
     * @param key   key
     * @param value header type with name as value
     */
    public void add(final HeaderType key, final HeaderType value) {
        add(key, new ByteArrayValue(value.getName()));
    }

    /**
     * Add header
     *
     * @param key   header type with name as key
     * @param value byte value
     */
    public void add(final HeaderType key, final ByteArrayValue value) {
        add(key.getName(), value);
    }

    /**
     * Add header
     *
     * @param key   key
     * @param value byte value
     */
    public void add(final String key, final ByteArrayValue value) {
        headers.add(new RecordHeader(key, value.getBytes()));
    }

    /**
     * Add header
     *
     * @param recordHeader header to add
     */
    public void add(final RecordHeader recordHeader) {
        headers.add(recordHeader);
    }

    /**
     * Add multiple headers
     *
     * @param recordHeaders list of headers
     */
    public void addAll(final RecordHeaders recordHeaders) {
        headers.addAll(recordHeaders.getList());
    }

    /**
     * Get all headers
     *
     * @return headers list
     */
    public List<Header> getList() {
        return headers;
    }

    /**
     * Stringify headers
     *
     * @return string with headers
     */
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
