package com.bbva.ddd.domain.commands.producers;

import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.producers.Producer;
import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.common.producers.record.PRecord;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.OptionalRecordHeaders;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
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
    private final String uuid;
    private final String key;

    private Command(final Producer producer, final String topic, final String action, final String key, final SpecificRecordBase value, final RecordHeaders headers, final String uuid) {
        this.producer = producer;
        this.topic = topic;
        this.action = action;
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.uuid = uuid;
    }

    public CommandRecordMetadata send(final ProducerCallback callback) {
        logger.debug("Sending command of type {}", action);

        final Future<RecordMetadata> result = producer.send(new PRecord(topic, key, value, headers), callback);

        final CommandRecordMetadata recordedMessageMetadata;
        try {
            final String commandUUID = headers.find(CommandHeaderType.UUID_KEY).asString();
            recordedMessageMetadata = new CommandRecordMetadata(result.get(), commandUUID, uuid);
            logger.info("CommandRecord created: {}", commandUUID);
        } catch (final InterruptedException | ExecutionException e) {
            logger.error("Cannot resolve the promise", e);
            throw new ProduceException("Cannot resolve the promise", e);

        }

        return recordedMessageMetadata;
    }

    public static class Builder {
        private String action;
        private SpecificRecordBase value;
        private OptionalRecordHeaders headers;
        private String uuid;
        private String to;
        private boolean persistent = false;
        private final CRecord referenceRecord;
        private final Producer producer;
        private final Boolean isReplay;

        public Builder(final Producer producer, final CRecord record, final Boolean isReplay) {
            this.producer = producer;
            referenceRecord = record;
            this.isReplay = isReplay;
        }

        public Builder(final CRecord record) {
            producer = new DefaultProducer(ConfigBuilder.get());
            isReplay = false;
            referenceRecord = record;
        }

        public Builder action(final String action) {
            this.action = action;
            return this;
        }

        public Builder value(final SpecificRecordBase value) {
            this.value = value;
            return this;
        }

        public Builder headers(final OptionalRecordHeaders headers) {
            this.headers = headers;
            return this;
        }

        public Builder uuid(final String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder persistent() {
            persistent = true;
            return this;
        }

        public Builder to(final String to) {
            this.to = to;
            return this;
        }

        public Command build() {
            if (uuid == null && action.equalsIgnoreCase(Command.CREATE_ACTION)) {
                uuid = UUID.randomUUID().toString();
            }
            final String key = UUID.randomUUID().toString();
            return new Command(producer, to, action, key, value, headers(action, key, uuid, referenceRecord, headers), uuid);
        }

        private RecordHeaders headers(final String name, final String key, final String entityUuid,
                                      final CRecord referenceRecord, final OptionalRecordHeaders optionalHeaders) {

            final RecordHeaders recordHeaders = new RecordHeaders();
            recordHeaders.add(CommonHeaderType.TYPE_KEY, CommandHeaderType.TYPE_VALUE);
            recordHeaders.add(CommandHeaderType.UUID_KEY, key);
            recordHeaders.add(CommandHeaderType.NAME_KEY, name);
            recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(isReplay && !persistent));
            if (entityUuid != null) {
                recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, entityUuid);
            }

            if (referenceRecord != null) {
                recordHeaders.add(CommonHeaderType.REFERENCE_RECORD_KEY_KEY, referenceRecord.key());
                recordHeaders.add(CommonHeaderType.REFERENCE_RECORD_TYPE_KEY,
                        referenceRecord.recordHeaders().find(CommonHeaderType.TYPE_KEY).asString());
                recordHeaders.add(CommonHeaderType.REFERENCE_RECORD_POSITION_KEY,
                        referenceRecord.topic() + "-" + referenceRecord.partition() + "-" + referenceRecord.offset());
            }

            if (optionalHeaders != null && optionalHeaders.getList().size() > 0) {
                recordHeaders.addAll(optionalHeaders);
            }

            logger.debug("CRecord headers: {}", recordHeaders.toString());

            return recordHeaders;
        }

    }

}
