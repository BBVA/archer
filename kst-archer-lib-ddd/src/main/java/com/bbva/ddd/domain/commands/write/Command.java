package com.bbva.ddd.domain.commands.write;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.PRecord;
import com.bbva.common.producers.ProducerCallback;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.OptionalRecordHeaders;
import com.bbva.common.utils.RecordHeaders;
import com.bbva.ddd.HelperDomain;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.reflect.InvocationTargetException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Command {

    public static final String CREATE_ACTION = "create";
    public static final String DELETE_ACTION = "delete";
    private static final String TYPE_COMMAND_VALUE = "command";

    private static final LoggerGen logger = LoggerGenesis.getLogger(Command.class.getName());
    private final CachedProducer producer;
    private final String topic;
    private final boolean persistent;

    public Command(final String topic, final ApplicationConfig applicationConfig, final boolean persistent) {
        this.topic = topic + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
        producer = new CachedProducer(applicationConfig);
        this.persistent = persistent;
    }

    /**
     * @param data
     * @param callback
     * @param <V>
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public <V extends SpecificRecord> CommandRecordMetadata create(final V data, final ProducerCallback callback)
            throws ExecutionException, InterruptedException {
        return create(data, null, callback);
    }

    /**
     * @param data
     * @param optionalHeaders
     * @param callback
     * @param <V>
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public <V extends SpecificRecord> CommandRecordMetadata create(final V data, final OptionalRecordHeaders optionalHeaders,
                                                                   final ProducerCallback callback) throws ExecutionException, InterruptedException {
        return generateCommand(Command.CREATE_ACTION, data, null, UUID.randomUUID().toString(), optionalHeaders,
                callback);
    }

    /**
     * @param data
     * @param entityId
     * @param callback
     * @param <V>
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public <V extends SpecificRecord> CommandRecordMetadata processAction(final String action, final String entityId, final V data,
                                                                          final ProducerCallback callback) throws ExecutionException, InterruptedException {
        return processAction(action, entityId, data, null, callback);
    }

    /**
     * @param action
     * @param entityId
     * @param data
     * @param optionalHeaders
     * @param callback
     * @param <V>
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public <V extends SpecificRecord> CommandRecordMetadata processAction(final String action, final String entityId, final V data,
                                                                          final OptionalRecordHeaders optionalHeaders, final ProducerCallback callback)
            throws ExecutionException, InterruptedException {
        return generateCommand(action, data, null, entityId, optionalHeaders, callback);
    }

    /**
     * Deprecated method. Use processAction method instead
     *
     * @param action
     * @param entityId
     * @param data
     * @param optionalHeaders
     * @param callback
     * @param <V>
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Deprecated
    public <V extends SpecificRecord> CommandRecordMetadata update(final String action, final String entityId, final V data,
                                                                   final OptionalRecordHeaders optionalHeaders, final ProducerCallback callback)
            throws ExecutionException, InterruptedException {
        if (entityId == null) {
            throw new IllegalArgumentException("entityId can not be null");
        }
        return generateCommand(action, data, null, entityId, optionalHeaders, callback);
    }

    /**
     * @param entityId
     * @param valueClass
     * @param callback
     * @param <V>
     * @return
     * @throws IllegalArgumentException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public <V extends SpecificRecord> CommandRecordMetadata delete(final String entityId, final Class<V> valueClass,
                                                                   final ProducerCallback callback) throws IllegalArgumentException, ExecutionException, InterruptedException {
        return delete(entityId, valueClass, null, callback);
    }

    /**
     * @param entityId
     * @param valueClass
     * @param optionalHeaders
     * @param callback
     * @param <V>
     * @return
     * @throws IllegalArgumentException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public <V extends SpecificRecord> CommandRecordMetadata delete(final String entityId, final Class<V> valueClass,
                                                                   final OptionalRecordHeaders optionalHeaders, final ProducerCallback callback)
            throws IllegalArgumentException, ExecutionException, InterruptedException {
        if (entityId == null) {
            throw new IllegalArgumentException("entityId can not be null");
        }
        return generateCommand(Command.DELETE_ACTION, null, valueClass, entityId, optionalHeaders, callback);
    }

    private <V extends SpecificRecord> CommandRecordMetadata generateCommand(final String action, final V record,
                                                                             final Class<V> recordClass, final String entityId, final OptionalRecordHeaders optionalHeaders, final ProducerCallback callback)
            throws InterruptedException, ExecutionException {
        logger.debug("Creating command of type " + action);
        final String key = UUID.randomUUID().toString();

        final RecordHeaders headers = headers(action, entityId, optionalHeaders);
        final String commandUUID = headers.find(CommandRecord.UUID_KEY).asString();

        Future<RecordMetadata> result = null;

        if (record != null) {
            result = producer.add(new PRecord<>(topic, key, record, headers), callback);
        } else if (recordClass != null) {
            try {
                result = producer.remove(new PRecord<>(topic, key, null, headers), recordClass, callback);
            } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException
                    | InstantiationException e) {
                logger.error("Error " + e.getMessage(), e);
            }
        } else {
            throw new IllegalArgumentException("Record or recordClass params must be set");
        }

        final CommandRecordMetadata recordedMessageMetadata = new CommandRecordMetadata(result.get(), commandUUID, entityId);

        logger.info("CommandRecord created: " + commandUUID);

        return recordedMessageMetadata;
    }

    private RecordHeaders headers(final String name, final String entityId, final OptionalRecordHeaders optionalHeaders) {

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CRecord.TYPE_KEY, new ByteArrayValue(Command.TYPE_COMMAND_VALUE));
        recordHeaders.add(CommandRecord.UUID_KEY, new ByteArrayValue(UUID.randomUUID().toString()));
        recordHeaders.add(CommandRecord.NAME_KEY, new ByteArrayValue(name));
        recordHeaders.add(CRecord.FLAG_REPLAY_KEY,
                new ByteArrayValue(HelperDomain.get().isReplayMode() && !persistent));
        if (entityId != null) {
            recordHeaders.add(CommandRecord.ENTITY_ID_KEY, new ByteArrayValue(entityId));
        }

        if (optionalHeaders != null && optionalHeaders.getList().size() > 0) {
            recordHeaders.addAll(optionalHeaders);
        }

        logger.debug("CRecord headers: " + recordHeaders.toString());

        return recordHeaders;
    }
}
