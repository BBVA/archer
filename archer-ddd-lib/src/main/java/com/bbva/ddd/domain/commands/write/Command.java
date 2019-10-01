package com.bbva.ddd.domain.commands.write;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.PRecord;
import com.bbva.common.producers.ProducerCallback;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.OptionalRecordHeaders;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
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
 * Manage the production of commands
 */
public class Command {

    public static final String CREATE_ACTION = "create";
    public static final String DELETE_ACTION = "delete";

    private static final Logger logger = LoggerFactory.getLogger(Command.class);
    private final CachedProducer producer;
    private final String topic;
    private final boolean persistent;

    /**
     * Constructor
     *
     * @param topic             topic name
     * @param applicationConfig configuration
     * @param persistent        flag to persist the actions
     */
    public Command(final String topic, final ApplicationConfig applicationConfig, final boolean persistent) {
        this.topic = topic + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
        producer = new CachedProducer(applicationConfig);
        this.persistent = persistent;
    }

    /**
     * Send data in a command stream. The action of the command will be Command.CREATE_ACTION
     *
     * @param value     Record data to send as command in event store
     * @param callback Callback executed when command is stored
     * @param <V>      Record type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata create(final V value, final ProducerCallback callback) {
        return create(value, null, null, callback);
    }

    /**
     * Send data in a command stream. The action of the command will be Command.CREATE_ACTION
     *
     * @param value     Record to send as command in event store
     * @param callback Callback executed when command is stored
     * @param <V>      Record type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata create(final V value, final CRecord referenceRecord, final ProducerCallback callback) {
        return create(value, referenceRecord, null, callback);
    }

    /**
     * Send data in a command stream. The action of the command will be Command.CREATE_ACTION
     *
     * @param value     Record to send as command in event store
     * @param callback Callback executed when command is stored
     * @param <V>      Record type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata create(final V value, final OptionalRecordHeaders optionalHeaders, final ProducerCallback callback) {
        return create(value, null, optionalHeaders, callback);
    }

    /**
     * Send data with optional headers in a command stream. The action of the command will be Command.CREATE_ACTION
     *
     * @param value            Data to send as command in event store
     * @param referenceRecord Reference record that triggers this event
     * @param optionalHeaders Optional headers for add to the command
     * @param callback        Callback executed when command is stored
     * @param <V>             Data type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata create(final V value, final CRecord referenceRecord,
                                                                   final OptionalRecordHeaders optionalHeaders,
                                                                   final ProducerCallback callback) {
        return generateCommand(Command.CREATE_ACTION, value, null, UUID.randomUUID().toString(), referenceRecord,
                optionalHeaders, callback);
    }

    /**
     * Send data  with specific action in a command stream.
     *
     * @param action     specific action for the command
     * @param entityUuid Entity affected
     * @param value       Data to send as command in event store
     * @param callback   Callback executed when command is stored
     * @param <V>        Data type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata processAction(final String action, final String entityUuid,
                                                                          final V value, final ProducerCallback callback) {
        return processAction(action, entityUuid, value, null, null, callback);
    }

    /**
     * Send data with specific action and optional headers in a command stream.
     *
     * @param action          Specific action for the command
     * @param entityUuid      Entity affected
     * @param value            Data to send as command in event store
     * @param referenceRecord Reference record that triggers this event
     * @param callback        Callback executed when command is stored
     * @param <V>             Data type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata processAction(final String action, final String entityUuid,
                                                                          final V value, final CRecord referenceRecord,
                                                                          final ProducerCallback callback) {
        return processAction(action, entityUuid, value, referenceRecord, null, callback);
    }

    /**
     * Send data with specific action and optional headers in a command stream.
     *
     * @param action          Specific action for the command
     * @param entityUuid      Entity affected
     * @param value            Data to send as command in event store
     * @param optionalHeaders Optional headers for add to the command
     * @param callback        Callback executed when command is stored
     * @param <V>             Data type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata processAction(final String action, final String entityUuid,
                                                                          final V value, final OptionalRecordHeaders optionalHeaders,
                                                                          final ProducerCallback callback) {
        return processAction(action, entityUuid, value, null, optionalHeaders, callback);
    }

    /**
     * Send data with specific action and optional headers in a command stream.
     *
     * @param action          Specific action for the command
     * @param entityUuid      Entity affected
     * @param value           Data to send as command in event store
     * @param referenceRecord Reference record that triggers this event
     * @param optionalHeaders Optional headers for add to the command
     * @param callback        Callback executed when command is stored
     * @param <V>             Data type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata processAction(final String action, final String entityUuid,
                                                                          final V value, final CRecord referenceRecord,
                                                                          final OptionalRecordHeaders optionalHeaders,
                                                                          final ProducerCallback callback) {
        return generateCommand(action, value, null, entityUuid, referenceRecord, optionalHeaders, callback);
    }

    /**
     * Send data in a command stream. The action of the command will be Command.DELETE_ACTION
     *
     * @param entityUuid Entity affected in the action
     * @param valueClass Class type of value
     * @param callback   Callback executed when command is stored
     * @param <V>        Data type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata delete(final String entityUuid, final Class<V> valueClass,
                                                                   final ProducerCallback callback) {
        return delete(entityUuid, valueClass, null, null, callback);
    }

    /**
     * Send data with optional headers in a command stream. The action of the command will be Command.DELETE_ACTION
     *
     * @param entityUuid      Entity affected in the action
     * @param valueClass      Class type of value
     * @param referenceRecord Reference record that triggers this event
     * @param callback        Callback executed when command is stored
     * @param <V>             Data type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata delete(final String entityUuid, final Class<V> valueClass,
                                                                   final CRecord referenceRecord, final ProducerCallback callback) {
        return delete(entityUuid, valueClass, referenceRecord, null, callback);
    }

    /**
     * Send data with optional headers in a command stream. The action of the command will be Command.DELETE_ACTION
     *
     * @param entityUuid      Entity affected in the action
     * @param valueClass      Class type of value
     * @param optionalHeaders Optional headers for add to the command
     * @param callback        Callback executed when command is stored
     * @param <V>             Data type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata delete(final String entityUuid, final Class<V> valueClass,
                                                                   final OptionalRecordHeaders optionalHeaders,
                                                                   final ProducerCallback callback) {
        return delete(entityUuid, valueClass, null, optionalHeaders, callback);
    }

    /**
     * Send data with optional headers in a command stream. The action of the command will be Command.DELETE_ACTION
     *
     * @param entityUuid      Entity affected in the action
     * @param valueClass      Class type of value
     * @param referenceRecord Reference record that triggers this event
     * @param optionalHeaders Optional headers for add to the command
     * @param callback        Callback executed when command is stored
     * @param <V>             Data type
     * @return A command record metadata
     */
    public <V extends SpecificRecord> CommandRecordMetadata delete(final String entityUuid, final Class<V> valueClass,
                                                                   final CRecord referenceRecord, final OptionalRecordHeaders optionalHeaders,
                                                                   final ProducerCallback callback) {
        if (entityUuid == null) {
            throw new ApplicationException("entityUuid can not be null");
        }
        return generateCommand(Command.DELETE_ACTION, null, valueClass, entityUuid, referenceRecord, optionalHeaders, callback);
    }

    private <V extends SpecificRecord> CommandRecordMetadata generateCommand(final String action, final V newValue,
                                                                             final Class<V> valueClass, final String entityUuid,
                                                                             final CRecord referenceRecord, final OptionalRecordHeaders optionalHeaders,
                                                                             final ProducerCallback callback) {
        logger.debug("Creating command of type {}", action);
        final String key = UUID.randomUUID().toString();

        final RecordHeaders headers = headers(action, key, entityUuid, referenceRecord, optionalHeaders);

        final Future<RecordMetadata> result;
        if (newValue != null) {
            result = producer.add(new PRecord<>(topic, key, newValue, headers), callback);
        } else if (valueClass != null) {
            result = producer.remove(new PRecord<>(topic, key, null, headers), valueClass, callback);
        } else {
            throw new ApplicationException("One of the params 'value' or 'valueClass' must be set");
        }

        final CommandRecordMetadata recordedMessageMetadata;
        try {
            final String commandUUID = headers.find(CommandHeaderType.UUID_KEY).asString();
            recordedMessageMetadata = new CommandRecordMetadata(result.get(), commandUUID, entityUuid);
            logger.info("CommandRecord created: {}", commandUUID);
        } catch (final InterruptedException | ExecutionException e) {
            logger.error("Cannot resolve the promise", e);
            throw new ProduceException("Cannot resolve the promise", e);

        }

        return recordedMessageMetadata;
    }

    private RecordHeaders headers(final String name, final String key, final String entityUuid,
                                  final CRecord referenceRecord, final OptionalRecordHeaders optionalHeaders) {

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.TYPE_KEY, CommandHeaderType.TYPE_VALUE);
        recordHeaders.add(CommandHeaderType.UUID_KEY, key);
        recordHeaders.add(CommandHeaderType.NAME_KEY, name);
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY,
                new ByteArrayValue(HelperDomain.get().isReplayMode() && !persistent));
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
