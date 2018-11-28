package com.bbva.common.producers;

import com.bbva.common.config.ApplicationConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

import java.util.concurrent.Future;

public class DefaultProducer<K, V> {

    private final ApplicationConfig applicationConfig;
    private final Logger logger;
    private final Producer<K, V> producer;

    public DefaultProducer(ApplicationConfig applicationConfig, Serializer<K> serializedKey,
            Serializer<V> serializedValue) {
        logger = Logger.getLogger(DefaultProducer.class);

        this.applicationConfig = applicationConfig;

        producer = new KafkaProducer<>(applicationConfig.producer().get(), serializedKey, serializedValue);
    }

    public Future<RecordMetadata> save(PRecord<K, V> record, ProducerCallback callback) {
        logger.debug("Produce generic PRecord with key " + record.key());

        Future<RecordMetadata> result = null;

        try {
            // if (PRecord.getList() != null) {
            // result = producer.send(new ProducerRecord<>(PRecord.topic(), null, PRecord.key(), PRecord.value(),
            // PRecord.getList().getList()), (metadata, e) -> {
            result = producer.send(record, (metadata, e) -> {
                if (e != null) {
                    logger.error("Error producing key " + record.key());
                    logger.error(e);
                } else {
                    logger.info("PRecord Produced. key " + record.key());
                }
                callback.onCompletion(record.key(), e);
                // producer.close(0, TimeUnit.MILLISECONDS);
            });
            // } else {
            // result = producer.send(new ProducerRecord<>(PRecord.topic(), PRecord.key(), PRecord.value()), (metadata,
            // e) -> {
            // logger.info("PRecord Produced. key " + PRecord.key());
            // callback.onCompletion(PRecord.key(), e);
            //// producer.close(0, TimeUnit.MILLISECONDS);
            // });
            // }
        } catch (Exception e) {
            logger.error("Exception producing record");
            logger.error(e);
            callback.onCompletion(record.key(), e);
        }

        producer.flush();

        logger.debug("End of production");

        return result;
    }

}
