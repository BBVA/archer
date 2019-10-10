package com.bbva.ddd.domain.events.write;

import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.common.producers.record.PRecord;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.common.utils.headers.types.EventHeaderType;
import com.bbva.ddd.domain.exceptions.ProduceException;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;
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
    private final String key;
    private final String producerName;
    private final SpecificRecordBase value;
    private final RecordHeaders headers;
    final CRecord referenceRecord;

    private Event(final CachedProducer producer, final String topic, final String key, final String producerName, final SpecificRecordBase value, final CRecord referenceRecord, final RecordHeaders headers) {
        this.producer = producer;
        this.topic = topic;
        this.key = key;
        this.producerName = producerName;
        this.value = value;
        this.referenceRecord = referenceRecord;
        this.headers = headers;
    }

    public EventRecordMetadata send(final ProducerCallback callback) {
        logger.debug("Generating event by {}", producerName);

        final Future<RecordMetadata> result = producer.add(new PRecord<>(topic, key, value, headers), callback);

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

    public static class Builder {

        private final CachedProducer producer;
        private String producerName;
        private String to;
        private String name;
        private String key;
        private SpecificRecordBase value;
        private final CRecord referenceRecord;
        private boolean replay = false;


        public Builder(final CRecord record) {
            producer = new CachedProducer(ConfigBuilder.get());
            referenceRecord = record;
        }

        public Builder(final CachedProducer producer, final CRecord record) {
            this.producer = producer;
            referenceRecord = record;
        }

        public Event.Builder producerName(final String producerName) {
            this.producerName = producerName;
            return this;
        }

        public Event.Builder to(final String to) {
            this.to = to;
            return this;
        }

        public Event.Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Event.Builder key(final String key) {
            this.key = key;
            return this;
        }

        public Event.Builder value(final SpecificRecordBase value) {
            this.value = value;
            return this;
        }

        public Event.Builder replay() {
            replay = true;
            return this;
        }

        public Event build() {
            final String key = this.key != null ? this.key : UUID.randomUUID().toString();

            return new Event(producer, to, key, producerName, value, referenceRecord,
                    headers(producerName, replay, referenceRecord, name));
        }

        private RecordHeaders headers(final String producerName, final boolean replay, final CRecord referenceRecord, final String name) {

            final RecordHeaders recordHeaders = new RecordHeaders();
            recordHeaders.add(CommonHeaderType.TYPE_KEY, EventHeaderType.TYPE_VALUE);
            recordHeaders.add(EventHeaderType.PRODUCER_NAME_KEY, producerName);
            recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, replay);

            if (referenceRecord != null) {
                final ByteArrayValue entityUuid = referenceRecord.recordHeaders().find(CommandHeaderType.ENTITY_UUID_KEY);
                if (entityUuid != null) {
                    recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, entityUuid.asString());
                }
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
}
