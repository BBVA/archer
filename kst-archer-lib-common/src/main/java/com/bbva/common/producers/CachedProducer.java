package com.bbva.common.producers;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.CustomCachedSchemaRegistryClient;
import com.bbva.common.utils.serdes.GenericAvroSerializer;
import com.bbva.common.utils.serdes.SpecificAvroSerializer;
import kst.logging.Logger;
import kst.logging.LoggerFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class CachedProducer {
    private static final Logger logger = LoggerFactory.getLogger(CachedProducer.class);

    private final Map<String, DefaultProducer> cachedProducers = new HashMap<>();
    private final ApplicationConfig applicationConfig;
    private final CustomCachedSchemaRegistryClient schemaRegistry;
    private final String schemaRegistryUrl;

    public CachedProducer(final ApplicationConfig applicationConfig) {
        this.applicationConfig = applicationConfig;

        schemaRegistryUrl = applicationConfig.get(ApplicationConfig.SCHEMA_REGISTRY_URL).toString();

        schemaRegistry = new CustomCachedSchemaRegistryClient(schemaRegistryUrl, 1000);
    }

    public <K, V> Future<RecordMetadata> add(final PRecord<K, V> record, final ProducerCallback callback) {

        final DefaultProducer<K, V> producer;

        if (cachedProducers.containsKey(record.topic())) {
            logger.info("Recovered cached producer for topic {}", record.topic());
            producer = cachedProducers.get(record.topic());

        } else {
            logger.info("Cached producer not found for topic {}", record.topic());
            final Map<String, String> serdeProps = Collections.singletonMap(ApplicationConfig.SCHEMA_REGISTRY_URL,
                    schemaRegistryUrl);

            final Serializer<K> serializedKey = serializeFrom(record.key());
            serializedKey.configure(serdeProps, true);
            logger.info("Serializing key to {}", serializedKey.toString());

            final Serializer<V> serializedValue = serializeFrom(record.value());
            serializedValue.configure(serdeProps, false);
            logger.info("Serializing value to {}", serializedValue.toString());

            producer = new DefaultProducer<>(applicationConfig, serializedKey, serializedValue);
            cachedProducers.put(record.topic(), producer);
        }

        return producer.save(record, callback);
    }

    public <K, V> Future<RecordMetadata> remove(final PRecord<K, V> record, final Class<V> valueClass, final ProducerCallback callback)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        final DefaultProducer<K, V> producer;

        if (cachedProducers.containsKey(record.topic())) {
            logger.info("Recovered cached producer for topic {}", record.topic());
            producer = cachedProducers.get(record.topic());

        } else {
            logger.info("Cached producer not found for topic {}", record.topic());
            final Map<String, String> serdeProps = Collections.singletonMap(ApplicationConfig.SCHEMA_REGISTRY_URL,
                    schemaRegistryUrl);

            final Serializer<K> serializedKey = serializeFrom(record.key());
            serializedKey.configure(serdeProps, true);
            logger.info("Serializing key to {}", serializedKey.toString());

            final V value = valueClass.getConstructor().newInstance();
            final Serializer<V> serializedValue = serializeFrom(value);
            serializedValue.configure(serdeProps, false);
            logger.info("Serializing value to {}", serializedValue.toString());

            producer = new DefaultProducer<>(applicationConfig, serializedKey, serializedValue);
            cachedProducers.put(record.topic(), producer);
        }

        return producer.save(record, callback);
    }

    private <T> Serializer<T> serializeFrom(final T type) {
        if (SpecificRecord.class.isAssignableFrom(type.getClass())) {
            return (Serializer<T>) new SpecificAvroSerializer<>(schemaRegistry);
        }

        if (GenericRecord.class.isAssignableFrom(type.getClass())) {
            return (Serializer<T>) new GenericAvroSerializer(schemaRegistry);
        }

        if (String.class.isAssignableFrom(type.getClass())) {
            return (Serializer<T>) Serdes.String().serializer();
        }

        if (Integer.class.isAssignableFrom(type.getClass())) {
            return (Serializer<T>) Serdes.Integer().serializer();
        }

        if (Long.class.isAssignableFrom(type.getClass())) {
            return (Serializer<T>) Serdes.Long().serializer();
        }

        if (Double.class.isAssignableFrom(type.getClass())) {
            return (Serializer<T>) Serdes.Double().serializer();
        }

        if (byte[].class.isAssignableFrom(type.getClass())) {
            return (Serializer<T>) Serdes.ByteArray().serializer();
        }

        if (ByteBuffer.class.isAssignableFrom(type.getClass())) {
            return (Serializer<T>) Serdes.ByteBuffer().serializer();
        }

        if (Bytes.class.isAssignableFrom(type.getClass())) {
            return (Serializer<T>) Serdes.Bytes().serializer();
        }

        throw new IllegalArgumentException("Unknown class for built-in serializer");
    }
}
