package com.bbva.ddd.domain.commands.producers;

import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.producers.Producer;
import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.common.producers.record.PRecord;
import com.bbva.common.utils.headers.OptionalRecordHeaders;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.ddd.domain.exceptions.ProduceException;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Manage the production of commands
 */
public class Command {

    public static final String CREATE_ACTION = "create";
    public static final String DELETE_ACTION = "delete";

    public enum Action {CREATE_ACTION, DELETE_ACTION}

    private static final Logger logger = LoggerFactory.getLogger(Command.class);
    private final Producer producer;
    private final String topic;
    private final String action;
    private final SpecificRecordBase value;
    private final RecordHeaders headers;
    private final String entityUuid;
    private final String key;

    private Command(final Producer producer, final String topic, final String action, final String key, final SpecificRecordBase value, final RecordHeaders headers, final String entityUuid) {
        this.producer = producer;
        this.topic = topic;
        this.action = action;
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.entityUuid = entityUuid;
    }

    /**
     * Send command to the event bus
     *
     * @param callback to manage the response of production
     * @return metadata of production
     */
    public CommandRecordMetadata send(final ProducerCallback callback) {
        logger.debug("Sending command of type {}", action);

        final Future<RecordMetadata> result = producer.send(new PRecord(topic, key, value, headers), callback);

        final CommandRecordMetadata recordedMessageMetadata;
        try {
//            final String commandUUID = headers.find(CommandHeaderType.KEY_KEY).asString();
            recordedMessageMetadata = new CommandRecordMetadata(result.get(), key, entityUuid);
            logger.info("CommandRecord created: {}", key);
        } catch (final InterruptedException | ExecutionException e) {
            logger.error("Cannot resolve the promise", e);
            throw new ProduceException("Cannot resolve the promise", e);

        }

        return recordedMessageMetadata;
    }

    /**
     * Manage the creation of commands
     */
    public static class Builder {
        private String action;
        private SpecificRecordBase value;
        private OptionalRecordHeaders headers;
        private String entityUuid;
        private String to;
        private boolean persistent = false;
        private final CRecord referenceRecord;
        private final Producer producer;
        private final Boolean isReplay;

        /**
         * Constructor
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
         * Constructor
         *
         * @param record consumed record
         */
        public Builder(final CRecord record) {
            producer = new DefaultProducer(ConfigBuilder.get());
            isReplay = false;
            referenceRecord = record;
        }

        /**
         * Set command action
         *
         * @param action the action
         * @return builder
         */
        public Builder action(final String action) {
            this.action = action;
            return this;
        }

        /**
         * Set command value
         *
         * @param value data value
         * @return builder
         */
        public Builder value(final SpecificRecordBase value) {
            this.value = value;
            return this;
        }

        /**
         * Set command headers
         *
         * @param headers command headers
         * @return builder
         */
        public Builder headers(final OptionalRecordHeaders headers) {
            this.headers = headers;
            return this;
        }

        /**
         * Set the entity uuid
         *
         * @param uuid the id
         * @return builder
         */
        public Builder entityUuid(final String uuid) {
            this.entityUuid = uuid;
            return this;
        }

        /**
         * Set flag of presistence to true
         *
         * @return builder
         */
        public Builder persistent() {
            persistent = true;
            return this;
        }

        /**
         * Set the source of the command
         *
         * @param to the source
         * @return builder
         */
        public Builder to(final String to) {
            this.to = to;
            return this;
        }

        /**
         * Build the command
         *
         * @return command instance
         */
        public Command build() {
            if (entityUuid == null && action.equalsIgnoreCase(Command.CREATE_ACTION)) {
                entityUuid = UUID.randomUUID().toString();
            }
            final String key = UUID.randomUUID().toString();
            return new Command(producer, to, action, key, value, headers(action, key, entityUuid, referenceRecord, headers), entityUuid);
        }

        private RecordHeaders headers(final String name, final String key, final String entityUuid,
                                      final CRecord referenceRecord, final OptionalRecordHeaders optionalHeaders) {

            final RecordHeaders recordHeaders = new RecordHeaders(CommandHeaderType.COMMAND_VALUE, isReplay && !persistent, referenceRecord);
            recordHeaders.add(CommandHeaderType.KEY_KEY, key);
            recordHeaders.add(CommandHeaderType.ACTION_KEY, name);
            if (entityUuid != null) {
                recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, entityUuid);
            }

            if (optionalHeaders != null && optionalHeaders.getList().size() > 0) {
                recordHeaders.addAll(optionalHeaders);
            }

            logger.debug("CRecord headers: {}", recordHeaders.toString());

            return recordHeaders;
        }

    }

}
