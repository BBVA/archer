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
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Avro serializer
 */
public class GenericAvroSerializer implements Serializer<GenericRecord> {

    KafkaAvroSerializer inner;

    /**
     * Constructor
     */
    public GenericAvroSerializer() {
        inner = new KafkaAvroSerializer();
    }

    /**
     * Constructor
     *
     * @param client schema registry client
     */
    public GenericAvroSerializer(final SchemaRegistryClient client) {
        inner = new KafkaAvroSerializer(client);
    }

    /**
     * Configure the serializer
     *
     * @param configs configuration
     * @param isKey   true/false
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        inner.configure(configs, isKey);
    }

    /**
     * Serialize method
     *
     * @param topic  topic name
     * @param record record
     * @return serialized data
     */
    @Override
    public byte[] serialize(final String topic, final GenericRecord record) {
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
