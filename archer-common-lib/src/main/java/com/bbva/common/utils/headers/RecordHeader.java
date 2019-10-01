package com.bbva.common.utils.headers;

import org.apache.kafka.common.header.Header;

/**
 * Record header
 */
public class RecordHeader implements Header {

    private String key;
    private byte[] value;

    /**
     * Header constructor
     *
     * @param key   header key
     * @param value header value
     */
    public RecordHeader(final String key, final byte[] value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Get header key
     *
     * @return key
     */
    @Override
    public String key() {
        return key;
    }

    /**
     * Set header key
     *
     * @param key value of key
     */
    public void key(final String key) {
        this.key = key;
    }

    /**
     * Get header value
     *
     * @return value data
     */
    @Override
    public byte[] value() {
        return value;
    }

    /**
     * Set header value
     *
     * @param value data value
     */
    public void value(final byte[] value) {
        this.value = value;
    }
}
