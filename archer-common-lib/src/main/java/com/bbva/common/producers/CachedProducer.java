package com.bbva.common.producers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.common.producers.record.PRecord;
import com.bbva.common.utils.serdes.GenericAvroSerializer;
import com.bbva.common.utils.serdes.SpecificAvroSerializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Improvement production caching the producers
 */
public class CachedProducer {

    private static final Logger logger = LoggerFactory.getLogger(CachedProducer.class);

    private final Map<String, Producer> cachedProducers = new HashMap<>();
    private final AppConfig appConfig;
    private final CachedSchemaRegistryClient schemaRegistry;
    private final String schemaRegistryUrl;

    /**
     * Constructor
     *
     * @param appConfig configuration
     */
    public CachedProducer(final AppConfig appConfig) {
        this.appConfig = appConfig;

        schemaRegistryUrl = appConfig.get(AppConfig.SCHEMA_REGISTRY_URL).toString();

        schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
    }

    /**
     * Add new record message
     *
     * @param record   record to send
     * @param callback callback to manage response
     * @param <K>      class type of key
     * @param <V>      class type of value
     * @return future with production result
     */
    public <K, V> Future<RecordMetadata> add(final PRecord<K, V> record, final ProducerCallback callback) {
        return getProducer(record, null).send(record, callback);
    }

    /**
     * Remove record message
     *
     * @param record     record to remove
     * @param valueClass value
     * @param callback   callback to manage response
     * @param <K>        class type of key
     * @param <V>        class type of value
     * @return future with production result
     */
    public <K, V> Future<RecordMetadata> remove(final PRecord<K, V> record, final Class<V> valueClass, final ProducerCallback callback) {
        return getProducer(record, valueClass).send(record, callback);
    }

    private <K, V> Producer<K, V> getProducer(final PRecord<K, V> record, final Class<V> valueClass) {
        final Producer<K, V> producer;

        if (appConfig.producer(AppConfig.ProducerProperties.ENABLE_IDEMPOTENCE).toString().equals("true")) {
            final String transactionalIdPrefix = appConfig.producer(AppConfig.ProducerProperties.TRANSACTIONAL_ID_PREFIX).toString();
            appConfig.producer().put(AppConfig.ProducerProperties.TRANSACTIONAL_ID, transactionalIdPrefix + record.topic());
        }

        if (cachedProducers.containsKey(record.topic())) {
            logger.info("Recovered cached producer for topic {}", record.topic());
            producer = cachedProducers.get(record.topic());

        } else {

            logger.info("Cached producer not found for topic {}", record.topic());
            final Map<String, String> serdeProps = Collections.singletonMap(AppConfig.SCHEMA_REGISTRY_URL,
                    schemaRegistryUrl);

            final Serializer<K> serializedKey = serializeFrom(record.key().getClass());
            serializedKey.configure(serdeProps, true);
            logger.info("Serializing key to {}", serializedKey.toString());

            final Serializer<V> serializedValue = serializeFrom(record.value() != null ? record.value().getClass() : valueClass);

            serializedValue.configure(serdeProps, false);
            logger.info("Serializing value to {}", serializedValue.toString());

            producer = new DefaultProducer<>(appConfig, serializedKey, serializedValue);
            cachedProducers.put(record.topic(), producer);
        }

        return producer;
    }

    private <T> Serializer<T> serializeFrom(final Class classType) {

        if (SpecificRecord.class.isAssignableFrom(classType)) {
            return (Serializer<T>) new SpecificAvroSerializer<>(schemaRegistry);
        }

        if (GenericRecord.class.isAssignableFrom(classType)) {
            return (Serializer<T>) new GenericAvroSerializer(schemaRegistry);
        }

        if (String.class.isAssignableFrom(classType)) {
            return (Serializer<T>) Serdes.String().serializer();
        }

        if (Integer.class.isAssignableFrom(classType)) {
            return (Serializer<T>) Serdes.Integer().serializer();
        }

        if (Long.class.isAssignableFrom(classType)) {
            return (Serializer<T>) Serdes.Long().serializer();
        }

        if (Double.class.isAssignableFrom(classType)) {
            return (Serializer<T>) Serdes.Double().serializer();
        }

        if (byte[].class.isAssignableFrom(classType)) {
            return (Serializer<T>) Serdes.ByteArray().serializer();
        }

        if (ByteBuffer.class.isAssignableFrom(classType)) {
            return (Serializer<T>) Serdes.ByteBuffer().serializer();
        }

        if (Bytes.class.isAssignableFrom(classType)) {
            return (Serializer<T>) Serdes.Bytes().serializer();
        }

        throw new ApplicationException("Unknown class for built-in serializer" + classType.getName());
    }
}
