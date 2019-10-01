package com.bbva.common.producers;

import com.bbva.common.config.ApplicationConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

/**
 * Default producer implementation
 *
 * @param <K> Type of Record schema
 * @param <V> Type of Record
 */
public class DefaultProducer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProducer.class);
    private final Producer<K, V> producer;
    private final boolean exactlyOnce;

    /**
     * Constructor
     *
     * @param applicationConfig general configuration
     * @param keySerializer     serializer for the key
     * @param valueSerializer   serializer for the value
     * @param exactlyOnce       exactly once enable
     */
    public DefaultProducer(final ApplicationConfig applicationConfig, final Serializer<K> keySerializer,
                           final Serializer<V> valueSerializer, final boolean exactlyOnce) {

        this.exactlyOnce = exactlyOnce;
        producer = new KafkaProducer<>(applicationConfig.producer().get(), keySerializer, valueSerializer);

        if (exactlyOnce) {
            producer.initTransactions();
        }
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

        Future<RecordMetadata> result = null;
        try {
            if (exactlyOnce) {
                producer.beginTransaction();
            }
            result = producer.send(record, (metadata, e) -> {
                if (e != null) {
                    logger.error("Error producing key " + record.key(), e);
                    if (exactlyOnce) {
                        producer.abortTransaction();
                    }
                } else {
                    logger.info("PRecord Produced. key {}", record.key());
                }
                callback.onCompletion(record.key(), e);
            });

            if (exactlyOnce) {
                producer.commitTransaction();
            }
//            producer.flush();

            logger.debug("End of production");

        } catch (final ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions,
            // so our only option is to close the producer and exit.
            throw e;
        } catch (final KafkaException e) {
            logger.error(e.getMessage(), e);
            if (exactlyOnce) {
                producer.abortTransaction();
            }
        } finally {
            producer.close();
        }
        return result;
    }

}
