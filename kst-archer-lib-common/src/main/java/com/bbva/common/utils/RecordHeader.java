package com.bbva.common.utils;

import org.apache.kafka.common.header.Header;

public class RecordHeader implements Header {

    private String key;
    private byte[] value;

    public RecordHeader() {

    }

    public RecordHeader(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public RecordHeader(String key, GenericValue value) {
        this.key = key;
        this.value = value.asByteArray();
    }

    @Override
    public String key() {
        return key;
    }

    public void key(String key) {
        this.key = key;
    }

    @Override
    public byte[] value() {
        return value;
    }

    public void value(byte[] value) {
        this.value = value;
    }
}
