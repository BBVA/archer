package com.bbva.common.utils.serdes;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.Map;

/**
 * Specific avro serde
 *
 * @param <T> type class of records
 */
public class SpecificAvroSerde<T extends SpecificRecord> implements Serde<T> {

    private final Serde<T> inner;

    /**
     * Constructor.
     */
    public SpecificAvroSerde() {
        inner = Serdes.serdeFrom(new SpecificAvroSerializer<>(), new SpecificAvroDeserializer<>());
    }

    /**
     * Constructor
     *
     * @param client schema registry client
     */
    public SpecificAvroSerde(final SchemaRegistryClient client) {
        this(client, Collections.emptyMap());
    }

    /**
     * Constructor
     *
     * @param client schema registry client
     * @param props  configurations
     */
    public SpecificAvroSerde(final SchemaRegistryClient client, final Map<String, ?> props) {
        inner = Serdes.serdeFrom(new SpecificAvroSerializer<>(client, props),
                new SpecificAvroDeserializer<>(client, props));
    }

    /**
     * Get the deserializer
     *
     * @return serializer
     */
    @Override
    public Serializer<T> serializer() {
        return inner.serializer();
    }

    /**
     * Get the deserializer
     *
     * @return deserializer
     */
    @Override
    public Deserializer<T> deserializer() {
        return inner.deserializer();
    }

    /**
     * Configure serde
     *
     * @param configs configurations
     * @param isKey   true/false
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    /**
     * Close the serde
     */
    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }

}
