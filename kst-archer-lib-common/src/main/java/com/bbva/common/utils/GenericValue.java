package com.bbva.common.utils;

import org.apache.kafka.common.utils.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

public class GenericValue {

    private byte[] value;

    public GenericValue(byte[] value) {
        this.value = value;
    }

    public GenericValue(ByteBuffer value) {
        value(value);
    }

    public GenericValue(Bytes value) {
        value(value);
    }

    public GenericValue(String value) {
        value(value);
    }

    public GenericValue(boolean value) {
        value(value);
    }

    public GenericValue(Integer value) {
        value(value);
    }

    public GenericValue(Long value) {
        value(value);
    }

    public GenericValue(Float value) {
        value(value);
    }

    public GenericValue(Object value) throws IOException {
        value(value);
    }

    public byte[] asByteArray() {
        return value;
    }

    public ByteBuffer asByteBuffer() {
        return ByteBuffer.wrap(value);
    }

    public Bytes asBytes() {
        return Bytes.wrap(value);
    }

    public String asString() {
        return new String(value);
    }

    public boolean asBoolean() {
        return value.length > 0 && value[0] != 0;
    }

    public Integer asInteger() {
        return asByteBuffer().getInt();
    }

    public Long asLong() {
        return asByteBuffer().getLong();
    }

    public Float asFloat() {
        return asByteBuffer().getFloat();
    }

    public void value(byte[] value) {
        this.value = value;
    }

    public void value(ByteBuffer value) {
        this.value = value.array();
    }

    public void value(Bytes value) {
        this.value = value.get();
    }

    public void value(String value) {
        this.value = value.getBytes();
    }

    public void value(boolean value) {
        this.value = new byte[] { (byte) (value ? 1 : 0) };
    }

    public void value(Integer value) {
        this.value = ByteBuffer.allocate(4).putInt(value).array();
    }

    public void value(Long value) {
        this.value = ByteBuffer.allocate(8).putLong(value).array();
    }

    public void value(Float value) {
        this.value = ByteBuffer.allocate(4).putFloat(value).array();
    }

    public void value(Object value) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;

        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(value);
            out.flush();
            this.value = bos.toByteArray();

        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }
}
