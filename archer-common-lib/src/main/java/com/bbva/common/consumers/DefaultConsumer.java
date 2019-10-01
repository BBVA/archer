package com.bbva.common.consumers;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.serdes.SpecificAvroSerde;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Default consumer implementation
 *
 * @param <V> Type of Record schema
 * @param <T> Type of Record
 */
public abstract class DefaultConsumer<V extends SpecificRecordBase, T extends CRecord> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultConsumer.class);

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected KafkaConsumer<String, V> consumer;
    protected final Collection<String> topics;
    protected final int id;
    protected Consumer<T> callback;
    private final ApplicationConfig applicationConfig;
    private final SpecificAvroSerde<V> specificSerde;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    /**
     * Constructor
     *
     * @param id                consuer id
     * @param topics            list of topics
     * @param callback          callback to manag responses
     * @param applicationConfig configuration
     */
    public DefaultConsumer(final int id, final List<String> topics, final Consumer<T> callback, final ApplicationConfig applicationConfig) {
        this.id = id;
        this.topics = topics;
        this.callback = callback;
        this.applicationConfig = applicationConfig;
        final String schemaRegistryUrl = applicationConfig.get(ApplicationConfig.SCHEMA_REGISTRY_URL).toString();

        final CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        final Map<String, String> serdeProps = Collections.singletonMap(ApplicationConfig.SCHEMA_REGISTRY_URL,
                schemaRegistryUrl);

        specificSerde = new SpecificAvroSerde<>(schemaRegistry, serdeProps);
        specificSerde.configure(serdeProps, false);
    }

    public abstract T message(String topic, int partition, long offset, long timestamp, TimestampType timestampType,
                              String key, V value, RecordHeaders headers);

    /**
     * Replay a list of topics
     *
     * @param topics list of topics
     */
    public void replay(final List<String> topics) {
        logger.info("Consumer started in replay mode");

        final Properties props = applicationConfig.consumer().get();
        props.put(ApplicationConfig.ConsumerProperties.ENABLE_AUTO_COMMIT, false);
        props.put(ApplicationConfig.ConsumerProperties.SESSION_TIMEOUT_MS, "30000");
        consumer = new KafkaConsumer<>(props, Serdes.String().deserializer(),
                specificSerde.deserializer());

        try {

            consumer.subscribe(topics/*, new HandleRebalanceListener()*/);
            logger.debug("Topics subscribed: {}", topics.toString());

            consumer.poll(Duration.ofMillis(1000));
            final Set<TopicPartition> topicPartitionSet = consumer.assignment();

            logger.debug("Partitions assigned {}", topicPartitionSet.toString());

            if (topicPartitionSet.isEmpty()) {
                logger.error("Replay failed. Not assigment detected");

            } else {
                for (final TopicPartition topicPartition : topicPartitionSet) {
                    logger.debug("Start replay on topic {} partition {}", topicPartition.topic(), topicPartition.partition());

                    final long lastOffset = consumer.position(topicPartition);

                    if (lastOffset > 0) {

                        consumer.seekToBeginning(Collections.singletonList(topicPartition));

                        long currentOffset;
                        boolean stop = false;
                        while (!stop) {
                            final ConsumerRecords<String, V> records = consumer.poll(Duration.ofMillis(3000));
                            if (records.isEmpty()) {
                                stop = true;
                            }
                            for (final ConsumerRecord<String, V> record : records) {
                                currentOffset = record.offset();
                                if (currentOffset <= lastOffset - 1) {
                                    callback.accept(message(record.topic(), record.partition(), record.offset(),
                                            record.timestamp(), record.timestampType(), record.key(), record.value(),
                                            new RecordHeaders(record.headers())));

                                    currentOffsets.put(topicPartition, new OffsetAndMetadata(currentOffset + 1));
                                    consumer.commitAsync(currentOffsets, (offsets, e) -> {
                                        if (e != null) {
                                            logger.error("Commit failed for offsets {}", offsets, e);
                                        }
                                    });

                                } else {
                                    stop = true;
                                    logger.debug("End replay on topic {} partition {}", topicPartition.topic(), topicPartition.partition());
                                    break;
                                }
                            }
                        }
                    }
                }
            }

        } finally {
            logger.info("End replay");
            consumer.close();
        }
    }

    /**
     * Start to consume records
     */
    public void play() {
        logger.info("Consumer started in normal mode");
        try {

            consumer = new KafkaConsumer<>(applicationConfig.consumer().get(), Serdes.String().deserializer(),
                    specificSerde.deserializer());
            consumer.subscribe(topics, new HandleRebalanceListener());

            while (!closed.get()) {
                final ConsumerRecords<String, V> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (final ConsumerRecord<String, V> record : records) {
                    callback.accept(message(record.topic(), record.partition(), record.offset(), record.timestamp(),
                            record.timestampType(), record.key(), record.value(), new RecordHeaders(record.headers())));

                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                    consumer.commitAsync(currentOffsets, (offsets, e) -> {
                        if (e != null) {
                            logger.error("Commit failed for offsets {}", offsets, e);
                        }
                    });
                }
            }
        } catch (final WakeupException e) {
            // ignore for shutdown
            if (!closed.get()) {
                logger.error("Error closing the consumer", e);
            }
        } catch (final Exception e) {
            logger.error("Unexpected error", e);
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
                logger.info("Closed consumer and we are done");
            }
        }
    }

    private class HandleRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
            logger.debug("Lost partitions in rebalance. Committing current offset:" + currentOffsets);
            consumer.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        }
    }

}
