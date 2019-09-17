package com.bbva.common.utils;

import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.utils.Bytes;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Serialize/deserialize the data in bytearray
 */
public class ByteArrayValue {
    private final byte[] data;

    /**
     * Constructor
     *
     * @param data       data to store
     * @param serialized is data serialized?
     */
    public ByteArrayValue(final byte[] data, final boolean serialized) {
        this.data = serialized ? data : Serde.serialize(data);
    }

    /**
     * Constructor
     *
     * @param data data toi serialize
     * @param <V>  Class type pof data
     */
    public <V> ByteArrayValue(final V data) {
        this.data = Serde.serialize(data);
    }

    /**
     * Get data in byte[]
     *
     * @return data
     */
    public byte[] getBytes() {
        return data;
    }

    /**
     * Get data as ByteArray
     *
     * @return data
     */
    public byte[] asByteArray() {
        return (byte[]) as();
    }

    /**
     * Get data as ByteBuffer
     *
     * @return data
     */
    public ByteBuffer asByteBuffer() {
        return (ByteBuffer) as();
    }

    /**
     * Get data as Bytes
     *
     * @return data
     */
    public Bytes asBytes() {
        return (Bytes) as();
    }

    /**
     * Get data as String
     *
     * @return data
     */
    public String asString() {
        return (String) as();
    }

    /**
     * Get data as boolean
     *
     * @return data
     */
    public boolean asBoolean() {
        return (Boolean) as();
    }

    /**
     * Get data as Integer
     *
     * @return data
     */
    public Integer asInteger() {
        return (Integer) as();
    }

    /**
     * Get data as long
     *
     * @return data
     */
    public Long asLong() {
        return (Long) as();
    }

    /**
     * Get data as Long
     *
     * @return data
     */
    public Float asFloat() {
        return (Float) as();
    }

    /**
     * Get data as object
     *
     * @return data
     */
    public Object as() {
        return Serde.deserializeAs(data);
    }

    /**
     * Serde Serialization/Deserialization
     */
    public static class Serde {

        /**
         * Deserialize byte[] to Object
         *
         * @param data bytes
         * @return deserialize object
         */
        public static Object deserializeAs(final byte[] data) {
            final GenericValue<Object> valueData = (GenericValue<Object>) SerializationUtils.deserialize(data);
            return valueData.getClassType().cast(valueData.getValue());
        }

        /**
         * Serialize data as byte[]
         *
         * @param data data
         * @param <T>  Type class of data
         * @return result bytes
         */
        public static <T> byte[] serialize(final T data) {
            return SerializationUtils.serialize(new GenericValue<>(data.getClass(), data));
        }
    }

    /**
     * Class to serialize
     *
     * @param <T> Type class of data
     */
    private static class GenericValue<T> implements Serializable {
        private static final long serialVersionUID = 5852838510474805025L;
        private final Class classType;
        private final T value;

        /**
         * Constructor
         *
         * @param classType Class type of the value
         * @param value     values
         */
        public GenericValue(final Class classType, final T value) {
            this.classType = classType;
            this.value = value;
        }

        /**
         * Get value object
         *
         * @return value
         */
        public T getValue() {
            return value;
        }

        /**
         * Get class typoe of the value
         *
         * @return class type
         */
        public Class getClassType() {
            return classType;
        }
    }
}
