package com.bbva.ddd.domain.events.write;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.PRecord;
import com.bbva.common.producers.ProducerCallback;
import com.bbva.common.utils.GenericValue;
import com.bbva.common.utils.RecordHeaders;
import com.bbva.ddd.ApplicationServices;
import com.bbva.ddd.domain.events.read.EventRecord;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Event {

    private static final String TYPE_EVENT_VALUE = "event";

    private static final LoggerGen logger = LoggerGenesis.getLogger(Event.class.getName());
    private final CachedProducer producer;
    private final String topic;

    public Event(String topicBaseName, ApplicationConfig applicationConfig) {
        this.topic = topicBaseName + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX;
        producer = new CachedProducer(applicationConfig);
    }

    /**
     *
     * @param productorName
     * @param data
     * @param callback
     * @param <V>
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public <V extends SpecificRecord> EventRecordMetadata send(String productorName, V data, ProducerCallback callback)
            throws ExecutionException, InterruptedException {
        return generateEvent(null, productorName, data, callback);
    }

    /**
     *
     * @param key
     * @param productorName
     * @param data
     * @param callback
     * @param <V>
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public <V extends SpecificRecord> EventRecordMetadata send(String key, String productorName, V data,
            ProducerCallback callback) throws ExecutionException, InterruptedException {
        return generateEvent(key, productorName, data, callback);
    }

    private <V extends SpecificRecord> EventRecordMetadata generateEvent(String key, String productorName, V record,
            ProducerCallback callback) throws InterruptedException, ExecutionException {
        logger.debug("Generating event by " + productorName);
        key = (key != null) ? key : UUID.randomUUID().toString();

        RecordHeaders headers = headers(productorName);

        Future<RecordMetadata> result = producer.add(new PRecord<>(topic, key, record, headers), callback);

        EventRecordMetadata recordedMessageMetadata = new EventRecordMetadata(result.get(), key);

        logger.info("Event created: " + key);

        return recordedMessageMetadata;
    }

    private RecordHeaders headers(String productorName) {

        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CRecord.TYPE_KEY, new GenericValue(Event.TYPE_EVENT_VALUE));
        recordHeaders.add(EventRecord.PRODUCTOR_NAME_KEY, new GenericValue(productorName));
        recordHeaders.add(CRecord.FLAG_REPLAY_KEY, new GenericValue(ApplicationServices.get().isReplayMode()));

        logger.debug("CRecord getList: " + recordHeaders.toString());

        return recordHeaders;
    }
}
