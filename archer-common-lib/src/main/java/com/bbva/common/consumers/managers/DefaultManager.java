package com.bbva.common.consumers.managers;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DefaultManager implements Manager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultManager.class);

    private final AppConfig appConfig;
    private final ConsumerCallback callback;
    private final KafkaConsumer<String, SpecificRecordBase> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public DefaultManager(final KafkaConsumer<String, SpecificRecordBase> consumer, final ConsumerCallback callback, final AppConfig appConfig) {
        this.appConfig = appConfig;
        this.callback = callback;
        this.consumer = consumer;
    }

    @Override
    public void process(final ConsumerRecords<String, SpecificRecordBase> records, final boolean replay) {

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

    private void commitOffsets() {
        if (!(boolean) appConfig.consumer(AppConfig.ConsumerProperties.ENABLE_AUTO_COMMIT)) {
            consumer.commitAsync(currentOffsets, (offsets, e) -> {
                if (e != null) {
                    logger.error("Commit failed for offsets " + offsets, e);
                }
            });
        }
    }
}
