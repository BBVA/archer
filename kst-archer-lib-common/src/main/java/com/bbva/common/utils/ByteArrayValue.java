package com.bbva.common.utils;

import org.apache.kafka.common.utils.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
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
            final T deserializeData;
            if (classType.getName().equals(ByteBuffer.class.getName())) {
                deserializeData = (T) ByteBuffer.wrap(data);
            } else if (classType.getName().equals(Bytes.class.getName())) {
                deserializeData = (T) Bytes.wrap(data);
            } else if (classType.getName().equals(String.class.getName())) {
                deserializeData = (T) new String(data);
            } else if (classType.getName().equals(boolean.class.getName())) {
                deserializeData = (T) Boolean.valueOf(data.length > 0 && data[0] != 0);
            } else if (classType.getName().equals(Integer.class.getName())) {
                deserializeData = (T) (Integer) ByteBuffer.wrap(data).getInt();
            } else if (classType.getName().equals(Long.class.getName())) {
                deserializeData = (T) (Long) ByteBuffer.wrap(data).getLong();
            } else if (classType.getName().equals(Float.class.getName())) {
                deserializeData = (T) (Float) ByteBuffer.wrap(data).getFloat();
            } else {
                throw new ClassCastException("Inconvertible types");
            }
            return deserializeData;
        }

        public static <T> byte[] serialize(final T data) {
            byte[] serializeData = null;
            if (data == byte[].class) {
                serializeData = (byte[]) data;
            } else if (data instanceof ByteBuffer) {
                serializeData = ((ByteBuffer) data).array();
            } else if (data instanceof Bytes) {
                serializeData = ((Bytes) data).get();
            } else if (data instanceof String) {
                serializeData = ((String) data).getBytes();
            } else if (data instanceof Boolean) {
                serializeData = new byte[]{(byte) ((Boolean) data ? 1 : 0)};
            } else if (data instanceof Integer) {
                serializeData = ByteBuffer.allocate(4).putInt((Integer) data).array();
            } else if (data instanceof Long) {
                serializeData = ByteBuffer.allocate(8).putLong((Long) data).array();
            } else if (data instanceof Float) {
                serializeData = ByteBuffer.allocate(4).putFloat((Float) data).array();
            } else {
                try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                    final ObjectOutput out;
                    out = new ObjectOutputStream(bos);
                    out.writeObject(data);
                    out.flush();
                    serializeData = bos.toByteArray();
                } catch (final IOException e) {
                    throw new ClassCastException("Inconvertible types: data is " + data.getClass().getName());
                }
            }
            return serializeData;
        }

    }
}
