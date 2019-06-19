package com.bbva.ddd.domain.events.write;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.PRecord;
import com.bbva.common.producers.ProducerCallback;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.common.utils.headers.types.EventHeaderType;
import com.bbva.ddd.domain.HelperDomain;
import com.bbva.ddd.domain.exceptions.ProduceException;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Event {

    private static final String TYPE_EVENT_VALUE = "event";

    private static final Logger logger = LoggerFactory.getLogger(Event.class);
    private final CachedProducer producer;
    private final String topic;

    public Event(final String topicBaseName, final ApplicationConfig applicationConfig) {
        this.topic = topicBaseName + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX;
        producer = new CachedProducer(applicationConfig);
    }

    public <V extends SpecificRecord> EventRecordMetadata send(final String productorName, final V data, final ProducerCallback callback) {
        return generateEvent(null, productorName, data, callback, HelperDomain.get().isReplayMode(), null, null);
    }

    public <V extends SpecificRecord> EventRecordMetadata send(final String productorName, final V data, final ProducerCallback callback, final String name) {
        return generateEvent(null, productorName, data, callback, HelperDomain.get().isReplayMode(), null, name);
    }

    public <V extends SpecificRecord> EventRecordMetadata send(final String key, final String productorName, final V data,
                                                               final ProducerCallback callback) {
        return generateEvent(key, productorName, data, callback, HelperDomain.get().isReplayMode(), null, null);
    }

    public <V extends SpecificRecord> EventRecordMetadata send(
            final String productorName, final V data, final ProducerCallback callback, final boolean replay, final String referenceId) {
        return generateEvent(null, productorName, data, callback, replay, referenceId, null);
    }

    private <V extends SpecificRecord> EventRecordMetadata generateEvent(
            String key, final String productorName, final V record,
            final ProducerCallback callback, final boolean replay, final String referenceId, final String name) {
        logger.debug("Generating event by {}", productorName);
        key = (key != null) ? key : UUID.randomUUID().toString();

        final RecordHeaders headers = headers(productorName, replay, referenceId, name);

        final Future<RecordMetadata> result = producer.add(new PRecord<>(topic, key, record, headers), callback);

        final EventRecordMetadata recordedMessageMetadata;
        try {
            recordedMessageMetadata = new EventRecordMetadata(result.get(), key);
        } catch (final InterruptedException | ExecutionException e) {
            logger.error("Cannot resolve the promise", e);
            throw new ProduceException("Cannot resolve the promise", e);
        }

        logger.info("Event created: {}", key);

        return recordedMessageMetadata;
    }

    private static RecordHeaders headers(final String productorName, final boolean replay, final String referenceId, final String name) {

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue(Event.TYPE_EVENT_VALUE));
        recordHeaders.add(EventHeaderType.PRODUCTOR_NAME_KEY, new ByteArrayValue(productorName));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(replay));
        if (referenceId != null) {
            recordHeaders.add(EventHeaderType.REFERENCE_ID_KEY, new ByteArrayValue(referenceId));
        }

        if (name != null) {
            recordHeaders.add(EventHeaderType.NAME_KEY, new ByteArrayValue(name));
        }

        logger.debug("CRecord getList: {}", recordHeaders.toString());

        return recordHeaders;
    }
}
