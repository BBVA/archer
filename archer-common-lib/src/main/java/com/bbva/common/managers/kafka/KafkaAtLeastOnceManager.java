package com.bbva.common.managers.kafka;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.callback.ConsumerCallback;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.producers.Producer;
import com.bbva.common.utils.headers.RecordHeaders;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class KafkaAtLeastOnceManager extends KafkaBaseManager {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAtLeastOnceManager.class);

    /**
     * Constructor
     *
     * @param sources   list of topics
     * @param callback  callback to manage responses
     * @param appConfig configuration
     */
    public KafkaAtLeastOnceManager(final List<String> sources, final ConsumerCallback callback, final AppConfig appConfig) {
        super(sources, callback, appConfig);
    }


    /**
     * Replay a list of topics
     *
     * @param topics list of topics
     */
    @Override
    public void replay(final List<String> topics) {
        logger.info("Consumer started in mode replay");

        final Properties props = appConfig.consumer();
        props.put(AppConfig.ConsumerProperties.ENABLE_AUTO_COMMIT, false);
        props.put(AppConfig.ConsumerProperties.SESSION_TIMEOUT_MS, "30000");

        final KafkaConsumer<String, SpecificRecordBase> replayConsumer = new KafkaConsumer<>(props, Serdes.String().deserializer(),
                specificSerde.deserializer());

        try {

            replayConsumer.subscribe(topics);
            logger.debug("Topics subscribed: {}", topics.toString());

            replayConsumer.poll(Duration.ZERO);
            final Set<TopicPartition> topicPartitionSet = replayConsumer.assignment();

            logger.debug("Partitions assigned {}", topicPartitionSet.toString());

            if (topicPartitionSet.isEmpty()) {
                logger.error("Replay failed. Not assigment detected");
            } else {

                for (final TopicPartition topicPartition : topicPartitionSet) {
                    logger.debug("Start replay on topic {} partition {}", topicPartition.topic(), topicPartition.partition());

                    final long lastOffset = replayConsumer.position(topicPartition);
                    final Producer producer = new DefaultProducer(appConfig);
                    if (lastOffset > 0) {

                        replayConsumer.seekToBeginning(Collections.singletonList(topicPartition));

                        replayConsumer.position(topicPartition);

                        long currentOffset;
                        boolean stop = false;
                        while (!stop) {
                            final ConsumerRecords<String, SpecificRecordBase> records = replayConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                            for (final ConsumerRecord<String, SpecificRecordBase> record : records) {
                                currentOffset = record.offset();
                                if (currentOffset <= lastOffset - 1) {
                                    callback.apply(new CRecord(record.topic(), record.partition(), record.offset(), record.timestamp(),
                                            record.timestampType(), record.key(), record.value(), new RecordHeaders(record.headers())), producer, false);

                                    if (currentOffset == lastOffset - 1) {
                                        final Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                                        offset.put(topicPartition, new OffsetAndMetadata(currentOffset + 1));
                                        replayConsumer.commitAsync(offset, null);
                                        stop = true;
                                        logger.debug("End replay on topic {} partition {}", topicPartition.topic(), topicPartition.partition());
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

        } finally {
            logger.info("End replay");
            replayConsumer.close();
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
            final Producer producer = new DefaultProducer(appConfig);

            while (!closed.get()) {
                final ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (final ConsumerRecord<String, SpecificRecordBase> record : records) {

                    callback.apply(new CRecord(record.topic(), record.partition(), record.offset(), record.timestamp(),
                            record.timestampType(), record.key(), record.value(), new RecordHeaders(record.headers())), producer, false);

                }
            }
        } catch (final WakeupException e) {
            // ignore for shutdown
            if (!closed.get()) {
                logger.error("Error closing the consumer", e);
            }
        } finally {
            consumer.close();
        }
    }

}
