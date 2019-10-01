package com.bbva.common.utils.serdes;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.Map;

/**
 * Avro serde to serialize/deserialize the data
 */
public class GenericAvroSerde implements Serde<GenericRecord> {

    private final Serde<GenericRecord> inner;

    /**
     * Constructor
     */
    public GenericAvroSerde() {
        inner = Serdes.serdeFrom(new GenericAvroSerializer(), new GenericAvroDeserializer());
    }

    /**
     * Constructor
     *
     * @param client schema registry client
     */
    public GenericAvroSerde(final SchemaRegistryClient client) {
        this(client, Collections.emptyMap());
    }

    /**
     * Constructor
     *
     * @param client schema registry client
     * @param props  configuration
     */
    public GenericAvroSerde(final SchemaRegistryClient client, final Map<String, ?> props) {
        inner = Serdes.serdeFrom(new GenericAvroSerializer(client), new GenericAvroDeserializer(client, props));
    }

    /**
     * Get internal serializer
     *
     * @return the serializer
     */
    @Override
    public Serializer<GenericRecord> serializer() {
        return inner.serializer();
    }

    /**
     * Get internal deserializer
     *
     * @return the deserializer
     */
    @Override
    public Deserializer<GenericRecord> deserializer() {
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
