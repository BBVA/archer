package com.bbva.ddd.domain.commands.write;

import com.bbva.common.config.ApplicationConfig;
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

public class Command {

    public static final String CREATE_ACTION = "create";
    public static final String DELETE_ACTION = "delete";

    private static final Logger logger = LoggerFactory.getLogger(Command.class);
    private final CachedProducer producer;
    private final String topic;
    private final boolean persistent;

    public Command(final String topic, final ApplicationConfig applicationConfig, final boolean persistent) {
        this.topic = topic + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
        producer = new CachedProducer(applicationConfig);
        this.persistent = persistent;
    }

    public <V extends SpecificRecord> CommandRecordMetadata create(final V data, final ProducerCallback callback) {
        return create(data, null, callback);
    }

    public <V extends SpecificRecord> CommandRecordMetadata create(final V data, final OptionalRecordHeaders optionalHeaders,
                                                                   final ProducerCallback callback) {
        return generateCommand(Command.CREATE_ACTION, data, null, UUID.randomUUID().toString(), optionalHeaders,
                callback);
    }

    public <V extends SpecificRecord> CommandRecordMetadata processAction(final String action, final String entityId, final V data,
                                                                          final ProducerCallback callback) {
        return processAction(action, entityId, data, null, callback);
    }

    public <V extends SpecificRecord> CommandRecordMetadata processAction(final String action, final String entityId, final V data,
                                                                          final OptionalRecordHeaders optionalHeaders, final ProducerCallback callback) {
        return generateCommand(action, data, null, entityId, optionalHeaders, callback);
    }

    public <V extends SpecificRecord> CommandRecordMetadata delete(final String entityId, final Class<V> valueClass,
                                                                   final ProducerCallback callback) {
        return delete(entityId, valueClass, null, callback);
    }

    public <V extends SpecificRecord> CommandRecordMetadata delete(final String entityId, final Class<V> valueClass,
                                                                   final OptionalRecordHeaders optionalHeaders, final ProducerCallback callback) {
        if (entityId == null) {
            throw new ApplicationException("entityUuid can not be null");
        }
        return generateCommand(Command.DELETE_ACTION, null, valueClass, entityId, optionalHeaders, callback);
    }

    private <V extends SpecificRecord> CommandRecordMetadata generateCommand(
            final String action, final V record,
            final Class<V> recordClass, final String entityId, final OptionalRecordHeaders optionalHeaders, final ProducerCallback callback) {
        logger.debug("Creating command of type {}", action);
        final String key = UUID.randomUUID().toString();

        final RecordHeaders headers = headers(action, key, entityId, optionalHeaders);
        final String commandUUID = headers.find(CommandHeaderType.UUID_KEY).asString();

        final Future<RecordMetadata> result;

        if (record != null) {
            result = producer.add(new PRecord<>(topic, key, record, headers), callback);
        } else if (recordClass != null) {

            result = producer.remove(new PRecord<>(topic, key, null, headers), recordClass, callback);

        } else {
            throw new ApplicationException("Record or recordClass params must be set");
        }

        final CommandRecordMetadata recordedMessageMetadata;
        try {
            recordedMessageMetadata = new CommandRecordMetadata(result.get(), commandUUID, entityId);
        } catch (final InterruptedException | ExecutionException e) {
            logger.error("Cannot resolve the promise", e);
            throw new ProduceException("Cannot resolve the promise", e);

        }

        logger.info("CommandRecord created: " + commandUUID);

        return recordedMessageMetadata;
    }

    private RecordHeaders headers(final String name, final String key, final String entityId, final OptionalRecordHeaders optionalHeaders) {

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue(CommandHeaderType.TYPE_VALUE));
        recordHeaders.add(CommandHeaderType.UUID_KEY, new ByteArrayValue(key));
        recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue(name));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY,
                new ByteArrayValue(HelperDomain.get().isReplayMode() && !persistent));
        if (entityId != null) {
            recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue(entityId));
        }

        if (optionalHeaders != null && optionalHeaders.getList().size() > 0) {
            recordHeaders.addAll(optionalHeaders);
        }

        logger.debug("CRecord headers: {}", recordHeaders.toString());

        return recordHeaders;
    }
}
