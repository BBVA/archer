package com.bbva.ddd.domain.events.write;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.PRecord;
import com.bbva.common.producers.ProducerCallback;
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

    private static final Logger logger = LoggerFactory.getLogger(Event.class);
    private final CachedProducer producer;
    private final String topic;

    public Event(final String topicBaseName, final ApplicationConfig applicationConfig) {
        topic = topicBaseName + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX;
        producer = new CachedProducer(applicationConfig);
    }

    public <V extends SpecificRecord> EventRecordMetadata send(final String producerName, final V data, final ProducerCallback callback) {
        return generateEvent(null, producerName, data, callback, HelperDomain.get().isReplayMode(), null, null);
    }

    public <V extends SpecificRecord> EventRecordMetadata send(final String producerName, final V data, final String name, final ProducerCallback callback) {
        return generateEvent(null, producerName, data, callback, HelperDomain.get().isReplayMode(), null, name);
    }

    public <V extends SpecificRecord> EventRecordMetadata send(final String key, final String producerName, final V data,
                                                               final ProducerCallback callback) {
        return generateEvent(key, producerName, data, callback, HelperDomain.get().isReplayMode(), null, null);
    }

    public <V extends SpecificRecord> EventRecordMetadata send(
            final String producerName, final V data, final boolean replay, final CRecord referenceRecord, final ProducerCallback callback) {
        return generateEvent(null, producerName, data, callback, replay, referenceRecord, null);
    }

    private <V extends SpecificRecord> EventRecordMetadata generateEvent(
            final String eventKey, final String producerName, final V record,
            final ProducerCallback callback, final boolean replay, final CRecord referenceRecord, final String name) {
        logger.debug("Generating event by {}", producerName);

        final String key = eventKey != null ? eventKey : UUID.randomUUID().toString();

        final RecordHeaders headers = headers(producerName, replay, referenceRecord, name);

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

    private static RecordHeaders headers(final String producerName, final boolean replay, final CRecord referenceRecord, final String name) {

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.TYPE_KEY, EventHeaderType.TYPE_VALUE);
        recordHeaders.add(EventHeaderType.PRODUCER_NAME_KEY, producerName);
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, replay);

        if (referenceRecord != null) {
            recordHeaders.add(CommonHeaderType.REFERENCE_RECORD_KEY_KEY, referenceRecord.key());
            recordHeaders.add(CommonHeaderType.REFERENCE_RECORD_TYPE_KEY,
                    referenceRecord.recordHeaders().find(CommonHeaderType.TYPE_KEY).asString());
            recordHeaders.add(CommonHeaderType.REFERENCE_RECORD_POSITION_KEY,
                    referenceRecord.topic() + "-" + referenceRecord.partition() + "-" + referenceRecord.offset());
        }

        if (name != null) {
            recordHeaders.add(EventHeaderType.NAME_KEY, name);
        }

        logger.debug("CRecord getList: {}", recordHeaders.toString());

        return recordHeaders;
    }
}
