package com.bbva.common.utils;

import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.utils.Bytes;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class ByteArrayValue {
    private final byte[] data;

    public ByteArrayValue(final byte[] data, final boolean serialized) {
        this.data = serialized ? data : Serde.serialize(data);
    }

    public <V> ByteArrayValue(final V data) {
        this.data = Serde.serialize(data);
    }

    public byte[] getBytes() {
        return data;
    }

    public byte[] asByteArray() {
        return (byte[]) as();
    }

    public ByteBuffer asByteBuffer() {
        return (ByteBuffer) as();
    }

    public Bytes asBytes() {
        return (Bytes) as();
    }

    public String asString() {
        return (String) as();
    }

    public boolean asBoolean() {
        return (Boolean) as();
    }

    public Integer asInteger() {
        return (Integer) as();
    }

    public Long asLong() {
        return (Long) as();
    }

    public Float asFloat() {
        return (Float) as();
    }

    public Object as() {
        return Serde.deserializeAs(data);
    }

    public static class Serde {

        public static Object deserializeAs(final byte[] data) {
            final GenericValue<Object> valueData = (GenericValue<Object>) SerializationUtils.deserialize(data);
            return valueData.getClassType().cast(valueData.getValue());
        }

        public static <T> byte[] serialize(final T data) {
            return SerializationUtils.serialize(new GenericValue<>(data.getClass(), data));
        }
    }

    private static class GenericValue<T> implements Serializable {
        private static final long serialVersionUID = 5852838510474805025L;
        private final Class classType;
        private final T value;

        public GenericValue(final Class classType, final T value) {
            this.classType = classType;
            this.value = value;
        }

        public T getValue() {
            return value;
        }

        public Class getClassType() {
            return classType;
        }
    }
}
