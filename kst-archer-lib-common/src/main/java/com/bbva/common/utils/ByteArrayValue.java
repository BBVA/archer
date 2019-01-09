package com.bbva.common.utils;

import org.apache.kafka.common.utils.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

public class ByteArrayValue {

    private byte[] data;

    public <V> ByteArrayValue(V data) {
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

    public <V> V as(Class<V> classValue) {
        return Serde.deserializeAs(classValue, data);
    }

    public static class Serde {

        @SuppressWarnings("unchecked")
        public static <T> T deserializeAs(Class<T> classType, byte[] data) {
            T deserializeData;
            if (classType.isInstance(ByteBuffer.class)) {
                deserializeData = (T) ByteBuffer.wrap(data);
            } else if (classType.isInstance(Bytes.class)) {
                deserializeData = (T) Bytes.wrap(data);
            } else if (classType.isInstance(String.class)) {
                deserializeData = (T) new String(data);
            } else if (classType.isInstance(boolean.class)) {
                deserializeData = (T) Boolean.valueOf(data.length > 0 && data[0] != 0);
            } else if (classType.isInstance(Integer.class)) {
                deserializeData = (T) (Integer) ByteBuffer.wrap(data).getInt();
            } else if (classType.isInstance(Long.class)) {
                deserializeData = (T) (Long) ByteBuffer.wrap(data).getLong();
            } else if (classType.isInstance(Float.class)) {
                deserializeData = (T) (Float) ByteBuffer.wrap(data).getFloat();
            } else {
                throw new ClassCastException("Inconvertible types");
            }
            return deserializeData;
        }

        @SuppressWarnings("unchecked")
        public static <T> byte[] serialize(T data) {
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
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                    ObjectOutput out;
                    out = new ObjectOutputStream(bos);
                    out.writeObject(data);
                    out.flush();
                    serializeData = bos.toByteArray();
                } catch (IOException e) {
                    throw new ClassCastException("Inconvertible types: data is " + data.getClass().getName());
                }
            }
            return serializeData;
        }

    }
}
