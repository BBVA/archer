package com.bbva.common.producers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.common.producers.record.PRecord;
import com.bbva.common.utils.serdes.SpecificAvroSerializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Transactional producer implementation.
 * <pre>
 *  {@code
 *      final DefaultProducer producer = new DefaultProducer(configuration, Serdes.String().serializer(), Serdes.String().serializer(), true);
 *      final Future result = producer.send(new PRecord<>("test", "key", "value", new RecordHeaders()), producerCallback);
 *  }
 * </pre>
 */
public class TransactionalProducer implements com.bbva.common.producers.Producer {

    private static final Logger logger = LoggerFactory.getLogger(TransactionalProducer.class);
    private final Producer producer;
    private List<CRecord> records;
    private final String groupId;

    /**
     * Constructor
     *
     * @param appConfig       general configuration
     * @param keySerializer   serializer for the key
     * @param valueSerializer serializer for the value
     */
    public TransactionalProducer(final AppConfig appConfig, final Serializer<String> keySerializer,
                                 final Serializer<SpecificRecordBase> valueSerializer) {

        groupId = (String) appConfig.consumer(AppConfig.ConsumerProperties.CONSUMER_GROUP_ID);
        producer = new KafkaProducer<>(appConfig.producer(), keySerializer, valueSerializer);
        producer.initTransactions();
    }

    /**
     * Constructor
     *
     * @param appConfig general configuration
     */
    public TransactionalProducer(final AppConfig appConfig) {
        final CachedSchemaRegistryClient schemaRegistry;
        final String schemaRegistryUrl = appConfig.get(AppConfig.SCHEMA_REGISTRY_URL).toString();
        schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);

        final Serializer keySerializer = Serdes.String().serializer();
        final Serializer valueSerializer = new SpecificAvroSerializer<>(schemaRegistry);

        groupId = (String) appConfig.consumer(AppConfig.ConsumerProperties.CONSUMER_GROUP_ID);

        producer = new KafkaProducer<>(appConfig.producer(), keySerializer, valueSerializer);
        producer.initTransactions();
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

    public void init(final List<CRecord> records) {
        producer.beginTransaction();
        this.records = records;
    }

    public void commit() {
        producer.sendOffsetsToTransaction(getUncommittedOffsets(), groupId);
        producer.commitTransaction();
        logger.debug("End of production");
    }

    private Map<TopicPartition, OffsetAndMetadata> getUncommittedOffsets() {
        final Map<TopicPartition, OffsetAndMetadata> uncommitedOffsets = new HashMap<>();
        for (final CRecord record : records) {
            uncommitedOffsets.put(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1));
        }

        return uncommitedOffsets;
    }

    public void abort() {
        producer.abortTransaction();
        logger.debug("Abort transaction");
    }

    @Override
    public void end() {
        producer.flush();
        producer.close();
        logger.debug("End of production");
    }
}
