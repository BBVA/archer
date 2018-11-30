package com.bbva.common.consumers;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.RecordHeaders;
import com.bbva.common.utils.serdes.SpecificAvroSerde;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public abstract class DefaultConsumer<V extends SpecificRecordBase, T extends CRecord> {

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected KafkaConsumer<String, V> consumer;
    protected final Collection<String> topics;
    protected final int id;
    protected Consumer<T> callback;
    private final ApplicationConfig applicationConfig;
    private final SpecificAvroSerde<V> specificSerde;
    private static final LoggerGen logger = LoggerGenesis.getLogger(DefaultConsumer.class.getName());

    public DefaultConsumer(int id, List<String> topics, Consumer<T> callback, ApplicationConfig applicationConfig) {
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

    @SuppressWarnings("unchecked")
    public void replay(List<String> topics) {
        logger.info("Consumer started in mode replay");

        Properties props = applicationConfig.consumer().get();
        props.put(ApplicationConfig.ConsumerProperties.ENABLE_AUTO_COMMIT, false);
        props.put(ApplicationConfig.ConsumerProperties.SESSION_TIMEOUT_MS, "30000");

        KafkaConsumer<String, V> replayConsumer = new KafkaConsumer<>(props, Serdes.String().deserializer(),
                specificSerde.deserializer());

        try {

            replayConsumer.subscribe(topics);
            logger.debug("Topics subscribed: " + topics.toString());

            replayConsumer.poll(0L);
            Set<TopicPartition> topicPartitionSet = replayConsumer.assignment();

            logger.debug("Partitions assigned " + topicPartitionSet.toString());

            if (topicPartitionSet.size() == 0) {
                logger.error("Replay failed. Not assigment detected");

            } else {

                for (TopicPartition topicPartition : topicPartitionSet) {
                    logger.debug("Start replay on topic " + topicPartition.topic() + " partition "
                            + topicPartition.partition());

                    long lastOffset = replayConsumer.position(topicPartition);

                    if (lastOffset > 0) {

                        replayConsumer.seekToBeginning(Arrays.asList(topicPartition));

                        long currentOffset = replayConsumer.position(topicPartition);

                        boolean stop = false;

                        while (!stop) {
                            ConsumerRecords<String, V> records = replayConsumer.poll(Long.MAX_VALUE);
                            for (ConsumerRecord<String, V> record : records) {
                                currentOffset = record.offset();
                                if (currentOffset <= lastOffset - 1) {
                                    callback.accept(message(record.topic(), record.partition(), record.offset(),
                                            record.timestamp(), record.timestampType(), record.key(), record.value(),
                                            new RecordHeaders(record.headers())));
                                    if (currentOffset == lastOffset - 1) {
                                        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                                        offset.put(topicPartition, new OffsetAndMetadata(currentOffset + 1));
                                        replayConsumer.commitAsync(offset, null);
                                        stop = true;
                                        logger.debug("End replay on topic " + topicPartition.topic() + " partition "
                                                + topicPartition.partition());
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Exception: " + e.getLocalizedMessage());
        } finally {
            logger.info("End replay");
            replayConsumer.close();
        }
    }

    public void play() {
        logger.info("Start normal play");
        try {
            consumer = new KafkaConsumer<>(applicationConfig.consumer().get(), Serdes.String().deserializer(),
                    specificSerde.deserializer());
            consumer.subscribe(topics);

            while (!closed.get()) {
                ConsumerRecords<String, V> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, V> record : records) {
                    callback.accept(message(record.topic(), record.partition(), record.offset(), record.timestamp(),
                            record.timestampType(), record.key(), record.value(), new RecordHeaders(record.headers())));
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
            if (!closed.get())
                logger.error(e);
        } catch (Exception e) {
            logger.error(e);
        } finally {
            consumer.close();
        }
    }

}