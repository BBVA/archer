package com.bbva.common.producers;

import com.bbva.common.config.ApplicationConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

/**
 * Default producer implementation.
 * <pre>
 *  {@code
 *      final DefaultProducer producer = new DefaultProducer(configuration, Serdes.String().serializer(), Serdes.String().serializer(), true);
 *      final Future result = producer.save(new PRecord<>("test", "key", "value", new RecordHeaders()), producerCallback);
 *  }
 * </pre>
 *
 * @param <K> Type of Record schema
 * @param <V> Type of Record
 */
public class DefaultProducer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProducer.class);
    private final Producer<K, V> producer;

    /**
     * Constructor
     *
     * @param applicationConfig general configuration
     * @param keySerializer     serializer for the key
     * @param valueSerializer   serializer for the value
     */
    public DefaultProducer(final ApplicationConfig applicationConfig, final Serializer<K> keySerializer,
                           final Serializer<V> valueSerializer) {

        producer = new KafkaProducer<>(applicationConfig.producer().get(), keySerializer, valueSerializer);
    }

    /**
     * Save the record
     *
     * @param record   record to save
     * @param callback callback to manag response
     * @return future with production result
     */
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
