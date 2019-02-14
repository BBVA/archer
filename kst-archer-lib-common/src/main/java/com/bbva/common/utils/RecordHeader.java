package com.bbva.common.utils;

import org.apache.kafka.common.header.Header;

public class RecordHeader implements Header {

    private String key;
    private byte[] value;

    public RecordHeader() {
    }

    public RecordHeader(final String key, final byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    public void key(final String key) {
        this.key = key;
    }

    @Override
    public byte[] value() {
        return value;
    }

    public void value(final byte[] value) {
        this.value = value;
    }
}
