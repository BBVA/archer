package com.bbva.ddd.domain.events.write;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
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

/**
 * Manage the production of events
 */
public class Event {

    private static final Logger logger = LoggerFactory.getLogger(Event.class);
    private final CachedProducer producer;
    private final String topic;

    /**
     * Constructor
     *
     * @param topicBaseName     to add specific suffix and interact with it
     * @param applicationConfig configuration
     */
    public Event(final String topicBaseName, final ApplicationConfig applicationConfig) {
        topic = topicBaseName + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX;
        producer = new CachedProducer(applicationConfig);
    }

    /**
     * Send data in a event stream.
     *
     * @param producerName producer identifier
     * @param data         data to send
     * @param callback     callback executed when command is stored
     * @param <V>          data type
     * @return A event record metadata
     */
    public <V extends SpecificRecord> EventRecordMetadata send(final String producerName, final V data, final ProducerCallback callback) {
        return generateEvent(null, producerName, data, callback, HelperDomain.get().isReplayMode(), null, null);
    }

    /**
     * Send data in a event stream.
     *
     * @param producerName producer identifier
     * @param data         data to send
     * @param name         header populated in EventHeaderType.NAME_KEY
     * @param callback     callback executed when command is stored
     * @param <V>          data type
     * @return A event record metadata
     */
    public <V extends SpecificRecord> EventRecordMetadata send(final String producerName, final V data, final String name, final ProducerCallback callback) {
        return generateEvent(null, producerName, data, callback, HelperDomain.get().isReplayMode(), null, name);
    }

    /**
     * Send data in a event stream with a specific key
     *
     * @param key          specific key
     * @param producerName producer identifier
     * @param data         data to send
     * @param callback     callback executed when command is stored
     * @param <V>          data type
     * @return A event record metadata
     */
    public <V extends SpecificRecord> EventRecordMetadata send(final String key, final String producerName, final V data,
                                                               final ProducerCallback callback) {
        return generateEvent(key, producerName, data, callback, HelperDomain.get().isReplayMode(), null, null);
    }

    /**
     * Send data in a event stream
     *
     * @param producerName    producer identifier
     * @param data            data to send
     * @param replay          flag replay to populate header CommonHeaderType.FLAG_REPLAY_KEY
     * @param referenceRecord reference command record
     * @param callback        callback executed when command is stored
     * @param <V>             data type
     * @return A event record metadata
     */
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
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue(EventHeaderType.TYPE_VALUE));
        recordHeaders.add(EventHeaderType.PRODUCER_NAME_KEY, new ByteArrayValue(producerName));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(replay));
        if (referenceRecord != null) {
            recordHeaders.add(EventHeaderType.REFERENCE_RECORD_KEY, new ByteArrayValue(referenceRecord));
        }

        if (name != null) {
            recordHeaders.add(EventHeaderType.NAME_KEY, new ByteArrayValue(name));
        }

        logger.debug("CRecord getList: {}", recordHeaders.toString());

        return recordHeaders;
    }
}
