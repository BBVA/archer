package com.bbva.common.managers.kafka;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.callback.ConsumerCallback;
import com.bbva.common.managers.Manager;
import com.bbva.common.utils.serdes.SpecificAvroSerde;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class KafkaBaseManager implements Manager {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBaseManager.class);

    protected final Collection<String> sources;
    protected final ConsumerCallback callback;
    protected final AppConfig appConfig;

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected KafkaConsumer<String, SpecificRecordBase> consumer;
    protected final SpecificAvroSerde<SpecificRecordBase> specificSerde;
    protected final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    /**
     * Constructor
     *
     * @param sources   list of topics
     * @param callback  callback to manage responses
     * @param appConfig configuration
     */
    public KafkaBaseManager(final List<String> sources, final ConsumerCallback callback, final AppConfig appConfig) {
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


    protected class HandleRebalanceListener implements ConsumerRebalanceListener {

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
