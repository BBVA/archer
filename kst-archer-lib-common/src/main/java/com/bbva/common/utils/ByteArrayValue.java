package com.bbva.common.utils;

import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.utils.Bytes;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class ByteArrayValue {

    private final byte[] data;

    public <V> ByteArrayValue(final V data) {
        this.data = Serde.serialize(data);
    }

    public byte[] getBytes() {
        return data;
    }

    public ByteBuffer asByteBuffer() {
        return as(ByteBuffer.class);
    }

    public Bytes asBytes() {
        return as(Bytes.class);
    }

    public String asString() {
        return as(String.class);
    }

    public boolean asBoolean() {
        return as(Boolean.class);
    }

    public Integer asInteger() {
        return as(Integer.class);
    }

    public Long asLong() {
        return as(Long.class);
    }

    public Float asFloat() {
        return as(Float.class);
    }

    public <V> V as(final Class<V> classValue) {
        return Serde.deserializeAs(classValue, data);
    }

    public static class Serde {

        public static <T> T deserializeAs(final Class<T> classType, final byte[] data) {
            return (T) SerializationUtils.deserialize(data);
        }

        public static <T> byte[] serialize(final T data) {
            if (data instanceof byte[]) {
                return (byte[]) data;
            }
            return SerializationUtils.serialize((Serializable) data);
        }

    }
}
