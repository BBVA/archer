/**
 * Copyright 2016 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bbva.common.utils.serdes;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Avro deserializer
 */
public class GenericAvroDeserializer implements Deserializer<GenericRecord> {

    KafkaAvroDeserializer inner;

    /**
     * Constructor
     */
    public GenericAvroDeserializer() {
        inner = new KafkaAvroDeserializer();
    }

    /**
     * Constructor
     *
     * @param client schema registry client
     */
    public GenericAvroDeserializer(final SchemaRegistryClient client) {
        inner = new KafkaAvroDeserializer(client);
    }

    /**
     * Constructor
     *
     * @param client schema registry client
     * @param props  properties
     */
    public GenericAvroDeserializer(final SchemaRegistryClient client, final Map<String, ?> props) {
        inner = new KafkaAvroDeserializer(client, props);
    }

    /**
     * Configure the deserializer
     *
     * @param configs configuration
     * @param isKey   true/false
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        inner.configure(configs, isKey);
    }

    /**
     * Deserialize
     *
     * @param s     key
     * @param bytes value
     * @return record serialized
     */
    @Override
    public GenericRecord deserialize(final String s, final byte[] bytes) {
        return (GenericRecord) inner.deserialize(s, bytes);
    }

    /**
     * Close the deserializer
     */
    @Override
    public void close() {
        inner.close();
    }
}
