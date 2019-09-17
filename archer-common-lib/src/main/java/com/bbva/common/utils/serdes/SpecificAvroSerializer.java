package com.bbva.common.utils.serdes;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;

/**
 * Specific avro serializer
 *
 * @param <T> Class type of records
 */
public class SpecificAvroSerializer<T extends SpecificRecord> implements Serializer<T> {

    KafkaAvroSerializer inner;

    /**
     * Constructor
     */
    public SpecificAvroSerializer() {
        inner = new KafkaAvroSerializer();
    }

    /**
     * Constructor
     *
     * @param client schema registry client
     */
    public SpecificAvroSerializer(final SchemaRegistryClient client) {
        inner = new KafkaAvroSerializer(client);
    }

    /**
     * Constructor
     *
     * @param client schema registry client
     * @param props  configuration
     */
    public SpecificAvroSerializer(final SchemaRegistryClient client, final Map<String, ?> props) {
        inner = new KafkaAvroSerializer(client, props);
    }

    /**
     * Configure the serializer
     *
     * @param configs configuration
     * @param isKey   true/false
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final Map<String, Object> effectiveConfigs = new HashMap<>(configs);
        effectiveConfigs.put(SPECIFIC_AVRO_READER_CONFIG, true);
        inner.configure(effectiveConfigs, isKey);
    }

    /**
     * Serialize the data
     *
     * @param topic  topic name
     * @param record record to serialize
     * @return serialized data
     */
    @Override
    public byte[] serialize(final String topic, final T record) {
        return inner.serialize(topic, record);
    }

    /**
     * Close the serializer
     */
    @Override
    public void close() {
        inner.close();
    }
}
