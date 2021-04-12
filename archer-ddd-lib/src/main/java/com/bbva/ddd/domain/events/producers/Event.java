package com.bbva.ddd.domain.events.producers;

import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.producers.Producer;
import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.common.producers.record.PRecord;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
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

    private final Producer producer;
    private final String topic;
    private final String key;
    private final String producerName;
    private final SpecificRecordBase value;
    private final RecordHeaders headers;

    private Event(final Producer producer, final String topic, final String key, final String producerName, final SpecificRecordBase value, final RecordHeaders headers) {
        this.producer = producer;
        this.topic = topic;
        this.key = key;
        this.producerName = producerName;
        this.value = value;
        this.headers = headers;
    }

    /**
     * Send event to the bus
     *
     * @param callback to manage response
     * @return metadata of the call
     */
    public EventRecordMetadata send(final ProducerCallback callback) {
        logger.debug("Generating event by {}", producerName);

        final Future<RecordMetadata> result = producer.send(new PRecord(topic, key, value, headers), callback);

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

    /**
     * Manage the creation of events
     */
    public static class Builder {

        private final Producer producer;
        private String producerName;
        private String to;
        private String name;
        private String key;
        private SpecificRecordBase value;
        private final CRecord referenceRecord;
        private final Boolean isReplay;

        /**
         * Builder constructor
         *
         * @param record consumed record
         */
        public Builder(final CRecord record) {
            producer = new DefaultProducer(ConfigBuilder.get());
            isReplay = false;
            referenceRecord = record;
        }

        /**
         * Builder constructor
         *
         * @param record   consumed record
         * @param producer producer instance
         * @param isReplay flag of replay
         */
        public Builder(final CRecord record, final Producer producer, final Boolean isReplay) {
            this.producer = producer;
            referenceRecord = record;
            this.isReplay = isReplay;
        }

        /**
         * Set producer name
         *
         * @param producerName the name
         * @return builder
         */
        public Event.Builder producerName(final String producerName) {
            this.producerName = producerName;
            return this;
        }

        /**
         * Set the source of the event
         *
         * @param to the name
         * @return builder
         */
        public Event.Builder to(final String to) {
            this.to = to;
            return this;
        }

        /**
         * Set the event name
         *
         * @param name the name
         * @return builder
         */
        public Event.Builder name(final String name) {
            this.name = name;
            return this;
        }

        /**
         * Set event key
         *
         * @param key event key
         * @return builder
         */
        public Event.Builder key(final String key) {
            this.key = key;
            return this;
        }

        /**
         * Set the event value
         *
         * @param value data value
         * @return builder
         */
        public Event.Builder value(final SpecificRecordBase value) {
            this.value = value;
            return this;
        }

        /**
         * Build the event instance
         *
         * @return event
         */
        public Event build() {
            final String eventKey = key != null ? key : UUID.randomUUID().toString();

            return new Event(producer, to, eventKey, producerName, value,
                    headers(producerName, referenceRecord, name));
        }

        private RecordHeaders headers(final String producerName, final CRecord referenceRecord, final String name) {

            final RecordHeaders recordHeaders = new RecordHeaders(EventHeaderType.EVENT_VALUE, isReplay, referenceRecord);
            recordHeaders.add(EventHeaderType.PRODUCER_NAME_KEY, producerName);

            if (referenceRecord != null) {
                final ByteArrayValue entityUuid = referenceRecord.recordHeaders().find(CommandHeaderType.ENTITY_UUID_KEY);
                if (entityUuid != null) {
                    recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, entityUuid.asString());
                }
            } else {
                recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, UUID.randomUUID().toString());
            }

            if (name != null) {
                recordHeaders.add(EventHeaderType.NAME_KEY, name);
            }

            logger.debug("CRecord getList: {}", recordHeaders.toString());

            return recordHeaders;
        }

    }
}
