package com.bbva.common.producers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.common.producers.record.PRecord;
import com.bbva.common.utils.serdes.SpecificAvroSerializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

/**
 * Default producer implementation.
 * <pre>
 *  {@code
 *      final Producer producer = new DefaultProducer(configuration, Serdes.String().serializer(), Serdes.String().serializer(), true);
 *      final Future result = producer.send(new PRecord<>("test", "key", "value", new RecordHeaders()), producerCallback);
 *  }
 * </pre>
 */
public class DefaultProducer implements com.bbva.common.producers.Producer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProducer.class);
    private final Producer producer;

    /**
     * Constructor
     *
     * @param appConfig       general configuration
     * @param keySerializer   serializer for the key
     * @param valueSerializer serializer for the value
     */
    public DefaultProducer(final AppConfig appConfig, final Serializer<String> keySerializer,
                           final Serializer<SpecificRecordBase> valueSerializer) {

        producer = new KafkaProducer<>(appConfig.producer(), keySerializer, valueSerializer);
    }

    /**
     * Constructor
     *
     * @param appConfig general configuration
     */
    public DefaultProducer(final AppConfig appConfig) {
        final CachedSchemaRegistryClient schemaRegistry;
        final String schemaRegistryUrl = appConfig.get(AppConfig.SCHEMA_REGISTRY_URL).toString();
        schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);

        final Serializer keySerializer = Serdes.String().serializer();
        final Serializer valueSerializer = new SpecificAvroSerializer<>(schemaRegistry);

        producer = new KafkaProducer<>(appConfig.producer(), keySerializer, valueSerializer);
    }

    @Override
    public Future<RecordMetadata> send(final PRecord record, final ProducerCallback callback) {
        logger.debug("Produce generic PRecord with key {}", record.key());

        return producer.send(record, (metadata, e) -> {
            if (e != null) {
                logger.error("Error producing key " + record.key(), e);
            } else {
                logger.info("PRecord Produced. key {}", record.key());
            }
            callback.onCompletion(record.key(), e);
        });

    }

    @Override
    public void end() {
        producer.flush();
        producer.close();
        logger.debug("End of production");
    }
}
