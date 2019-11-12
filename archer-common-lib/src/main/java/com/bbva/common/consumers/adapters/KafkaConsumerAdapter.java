package com.bbva.common.consumers.adapters;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.callback.ConsumerCallback;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.producers.Producer;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.serdes.SpecificAvroSerde;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerAdapter implements ConsumerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerAdapter.class);

    private final Collection<String> sources;
    private final ConsumerCallback callback;
    private final AppConfig appConfig;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaConsumer<String, SpecificRecordBase> consumer;
    private final SpecificAvroSerde<SpecificRecordBase> specificSerde;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    /**
     * Constructor
     *
     * @param sources   list of topics
     * @param callback  callback to manage responses
     * @param appConfig configuration
     */
    public KafkaConsumerAdapter(final List<String> sources, final ConsumerCallback callback, final AppConfig appConfig) {
        this.sources = sources;
        this.callback = callback;
        this.appConfig = appConfig;

        final String schemaRegistryUrl = appConfig.get(AppConfig.SCHEMA_REGISTRY_URL).toString();

        final CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        final Map<String, String> serdeProps = Collections.singletonMap(AppConfig.SCHEMA_REGISTRY_URL,
                schemaRegistryUrl);

        specificSerde = new SpecificAvroSerde<>(schemaRegistry, serdeProps);
        specificSerde.configure(serdeProps, false);

    }


    /**
     * Replay a list of topics
     *
     * @param topics list of topics
     */
    @Override
    public void replay(final List<String> topics) {
        logger.info("Consumer started in replay mode");

        final Properties props = appConfig.consumer();
        props.put(AppConfig.ConsumerProperties.ENABLE_AUTO_COMMIT, false);
        props.put(AppConfig.ConsumerProperties.SESSION_TIMEOUT_MS, "30000");
        consumer = new KafkaConsumer<>(props, Serdes.String().deserializer(),
                specificSerde.deserializer());

        try {

            consumer.subscribe(topics);
            logger.debug("Topics subscribed: {}", topics);

            consumer.poll(Duration.ofMillis(1000));
            final Set<TopicPartition> topicPartitionSet = consumer.assignment();

            logger.debug("Partitions assigned {}", topicPartitionSet);

            if (topicPartitionSet.isEmpty()) {
                logger.error("Replay failed. Not assignment detected");

            } else {
                for (final TopicPartition topicPartition : topicPartitionSet) {
                    logger.debug("Start replay on topic {} partition {}", topicPartition.topic(), topicPartition.partition());

                    final long lastOffset = consumer.position(topicPartition);

                    if (lastOffset > 0) {

                        consumer.seekToBeginning(Collections.singletonList(topicPartition));

                        long currentOffset;
                        boolean stop = false;
                        while (!stop) {
                            final ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(Duration.ofMillis(3000));
                            if (records.isEmpty()) {
                                stop = true;
                            }
                            for (final ConsumerRecord<String, SpecificRecordBase> record : records) {
                                currentOffset = record.offset();
                                if (currentOffset <= lastOffset - 1) {
                                    callback.apply(new CRecord(record.topic(), record.partition(), record.offset(),
                                            record.timestamp(), record.timestampType(), record.key(), record.value(),
                                            new RecordHeaders(record.headers())), new DefaultProducer(appConfig), true);

                                    currentOffsets.put(topicPartition, new OffsetAndMetadata(currentOffset + 1));
                                    commitOffsets();
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
    @Override
    public void play() {
        logger.info("Consumer started in normal mode");
        try {

            consumer = new KafkaConsumer<>(appConfig.consumer(), Serdes.String().deserializer(),
                    specificSerde.deserializer());
            consumer.subscribe(sources, new HandleRebalanceListener());

            while (!closed.get()) {
                final ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (final ConsumerRecord<String, SpecificRecordBase> record : records) {
                    final Producer producer = new DefaultProducer(appConfig);

                    //Init the transaction
                    producer.init();

                    callback.apply(new CRecord(record.topic(), record.partition(), record.offset(), record.timestamp(),
                            record.timestampType(), record.key(), record.value(), new RecordHeaders(record.headers())), producer, false);

                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));

                    commitOffsets();

                    //End the transaction
                    producer.end();
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

    private void commitOffsets() {
        if (!(boolean) appConfig.consumer(AppConfig.ConsumerProperties.ENABLE_AUTO_COMMIT)) {
            consumer.commitAsync(currentOffsets, (offsets, e) -> {
                if (e != null) {
                    logger.error("Commit failed for offsets " + offsets, e);
                }
            });
        }
    }

    private class HandleRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
            logger.debug("Lost partitions in rebalance. Committing current offset: {}", currentOffsets);
            consumer.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        }
    }

    /**
     * Close the consumer
     */
    @Override
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
