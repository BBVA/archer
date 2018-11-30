package com.bbva.common.producers;

import com.bbva.common.config.ApplicationConfig;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import java.util.concurrent.Future;

public class DefaultProducer<K, V> {

    //TODO private variable never used
    private final ApplicationConfig applicationConfig;
    private static final LoggerGen logger = LoggerGenesis.getLogger(DefaultProducer.class.getName());
    private final Producer<K, V> producer;

    public DefaultProducer(ApplicationConfig applicationConfig, Serializer<K> serializedKey,
            Serializer<V> serializedValue) {

        this.applicationConfig = applicationConfig;

        producer = new KafkaProducer<>(applicationConfig.producer().get(), serializedKey, serializedValue);
    }

    public Future<RecordMetadata> save(PRecord<K, V> record, ProducerCallback callback) {
        logger.debug("Produce generic PRecord with key " + record.key());

        Future<RecordMetadata> result = null;

        try {
            result = producer.send(record, (metadata, e) -> {
                if (e != null) {
                    logger.error("Error producing key " + record.key());
                    logger.error(e);
                } else {
                    logger.info("PRecord Produced. key " + record.key());
                }
                callback.onCompletion(record.key(), e);
            });
        } catch (Exception e) {
            logger.error("Exception producing record", e);
            callback.onCompletion(record.key(), e);
        }

        producer.flush();

        logger.debug("End of production");

        return result;
    }

}
