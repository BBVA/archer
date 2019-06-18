package com.bbva.common.producers;

import com.bbva.common.config.ApplicationConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

public class DefaultProducer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProducer.class);
    private final Producer<K, V> producer;

    public DefaultProducer(final ApplicationConfig applicationConfig, final Serializer<K> serializedKey,
                           final Serializer<V> serializedValue) {

        producer = new KafkaProducer<>(applicationConfig.producer().get(), serializedKey, serializedValue);
    }

    public Future<RecordMetadata> save(final PRecord<K, V> record, final ProducerCallback callback) {
        logger.debug("Produce generic PRecord with key {}", record.key());

        final Future<RecordMetadata> result = producer.send(record, (metadata, e) -> {
            if (e != null) {
                logger.error("Error producing key " + record.key(), e);
            } else {
                logger.info("PRecord Produced. key {}", record.key());
            }
            callback.onCompletion(record.key(), e);
        });

        producer.flush();

        logger.debug("End of production");

        return result;
    }

}
