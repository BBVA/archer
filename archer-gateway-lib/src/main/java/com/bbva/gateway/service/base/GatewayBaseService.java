package com.bbva.gateway.service.base;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.dataprocessors.states.States;
import com.bbva.ddd.domain.handlers.contexts.HandlerContext;
import com.bbva.gateway.aggregates.GatewayAggregate;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.service.GatewayService;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.LinkedHashMap;
import java.util.UUID;

import static com.bbva.gateway.constants.Constants.INTERNAL_SUFFIX;
import static com.bbva.gateway.constants.Constants.KEY_SUFFIX;

/**
 * Gateway service implementation
 *
 * @param <R> Response type
 */
public abstract class GatewayBaseService<R> implements GatewayService<R> {
    private static final Logger logger = LoggerFactory.getLogger(GatewayBaseService.class);

    protected GatewayConfig config;
    protected static ObjectMapper om = new ObjectMapper();
    private boolean retryEnabled = false;
    private int seconds;
    private int attempts;
//    protected static DefaultProducer producer;

//    /**
//     * Send event with the original record and response
//     *
//     * @param originalRecord original record
//     * @param outputEvent    response
//     * @param <O>            Output type
//     */
//    protected static <O extends SpecificRecordBase> void sendEvent(final CRecord originalRecord, final O outputEvent) {
//        sendEvent(baseName, originalRecord, outputEvent);
//    }
//
//    /**
//     * Send an event to the event-store as result of the operation
//     *
//     * @param eventBaseName  base name
//     * @param originalRecord original record
//     * @param outputEvent    output
//     * @param <O>            Output type
//     */
//    protected static <O extends SpecificRecordBase> void sendEvent(final String eventBaseName, final CRecord originalRecord, final O outputEvent) {
//        boolean replay = false;
//        if (originalRecord != null && isReplay(originalRecord)) {
//            replay = true;
//        }
//        final Event.Builder eventBuilder = new Event.Builder(originalRecord, producer, replay)
//                .to(eventBaseName).producerName("gateway").value(outputEvent);
//
//
//        eventBuilder.build().send(GatewayBaseService::handleOutPutted);
//    }

//    /**
//     * Check if the record is in replay
//     *
//     * @param record record
//     * @return true/false
//     */
//    protected static Boolean isReplay(final CRecord record) {
//        return record.recordHeaders() != null && record.isReplayMode();
//    }

//    /**
//     * Manage output response
//     *
//     * @param o Output
//     * @param e Exception
//     */
//    protected static void handleOutPutted(final Object o, final Exception e) {
//        if (e != null) {
//            logger.error("Error sending event", e);
//        } else {
//            logger.info("Event sent {}", o);
//        }
//    }

    /**
     * Call to external component
     *
     * @param record record
     * @return response
     */
    protected abstract R call(CRecord record);

    /**
     * Initialize the service
     */
    protected abstract void init();

    /**
     * Check if the call is success in the response
     *
     * @param response call response
     * @return true/false
     */
    protected abstract boolean isSuccess(R response);

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(final GatewayConfig configuration) {
        config = configuration;

        final LinkedHashMap<String, Object> retryPolicy = config.gateway(GatewayConfig.GatewayProperties.GATEWAY_RETRY) != null ? (LinkedHashMap<String, Object>) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_RETRY) : null;
        retryEnabled = retryPolicy != null && (boolean) retryPolicy.get(GatewayConfig.GatewayProperties.GATEWAY_RETRY_ENABLED);
        if (retryEnabled) {
            seconds = Integer.parseInt(retryPolicy.get(GatewayConfig.GatewayProperties.GATEWAY_RETRY_ATTEMPT_SECONDS).toString());
            attempts = Integer.valueOf(retryPolicy.get(GatewayConfig.GatewayProperties.GATEWAY_RETRY_ATTEMPTS).toString());
        }

        init();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecord(final HandlerContext context) {
        final CRecord record = context.consumedRecord();
        if (context.isReplay()) {
            final TransactionChangelog transactionChangelog = findChangelogByReference(record);

            if (transactionChangelog != null) {
                final R response = parseChangelogFromString(transactionChangelog.getOutput());
                processResponse(context, response);
            }
        } else {
            final R response = makeExternalRequest(record, 0);
            saveChangelog(context, response);
            processResponse(context, response);
        }
    }

    /**
     * Call makeExternalRequest
     *
     * @param record     record for call
     * @param attemptNum number of makeExternalRequest
     * @return response
     */
    protected R makeExternalRequest(final CRecord record, final int attemptNum) {
        final R response = call(record);

        if ((response == null || !isSuccess(response)) && retryEnabled && attemptNum < attempts) {
            try {
                Thread.sleep(seconds);
            } catch (final InterruptedException e) { //NOSONAR
                logger.error("Problems sleeping the thread", e);
            }
            return makeExternalRequest(record, attemptNum + 1);
        }
        return response;
    }

    /**
     * Parse output string to response
     *
     * @param output json string
     * @return response
     */
    protected R parseChangelogFromString(final String output) {
        try {
            final Class classType = (Class<R>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            return (R) om.readValue(output, classType);
        } catch (final IOException | IllegalArgumentException e) {
            logger.error("Cannot parse changelog to string", e);
            return null;
        }
    }

    /**
     * Parse response to string
     *
     * @param response response
     * @return json string
     */
    protected String parseChangelogToString(final R response) {
        try {
            return om.writeValueAsString(response);
        } catch (final IOException e) {
            logger.error("Cannot parse changelog objects to string", e);
            return "";
        }
    }

    /**
     * Save a changelog with original record and response
     *
     * @param context  original context
     * @param response response
     */
    protected void saveChangelog(final HandlerContext context, final R response) {
        final CRecord originalRecord = context.consumedRecord();
        final String key = UUID.randomUUID().toString();
        final TransactionChangelog transactionChangelog = new TransactionChangelog(key, originalRecord.value().toString(), parseChangelogToString(response));

        context.repository().create(GatewayAggregate.class, key, transactionChangelog, (id, e) -> {
            if (e != null) {
                logger.error("Error adding changelog", e);
            } else {
                logger.info("Response has been saved in event store as a transaction changelog with key {}", id);
            }
        });
    }

    /**
     * Search a changelog by a reference
     *
     * @param record record with reference
     * @return changelog
     */
    protected TransactionChangelog findChangelogByReference(final CRecord record) {
        return (TransactionChangelog) States.get().getStore(INTERNAL_SUFFIX + KEY_SUFFIX).findById(record.recordHeaders().find(CommandHeaderType.ENTITY_UUID_KEY.getName()).asString());
    }
}
