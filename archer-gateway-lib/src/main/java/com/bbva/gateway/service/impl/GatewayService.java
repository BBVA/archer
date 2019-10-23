package com.bbva.gateway.service.impl;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.dataprocessors.states.States;
import com.bbva.ddd.domain.events.producers.Event;
import com.bbva.ddd.domain.handlers.contexts.HandlerContextImpl;
import com.bbva.gateway.aggregates.GatewayAggregate;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.service.IGatewayService;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;
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
 * @param <T> Response type
 */
public abstract class GatewayService<T>
        implements IGatewayService<T> {
    private static final Logger logger = LoggerFactory.getLogger(GatewayService.class);

    protected GatewayConfig config;
    protected static ObjectMapper om = new ObjectMapper();
    private Boolean retryEnabled = false;
    private int seconds;
    private int attemps;
    protected static String baseName;
    protected static DefaultProducer producer;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final GatewayConfig configuration, final String gatewayBaseName) {
        config = configuration;

        final LinkedHashMap<String, Object> retryPolicy = config.gateway(GatewayConfig.GatewayProperties.GATEWAY_RETRY) != null ? (LinkedHashMap<String, Object>) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_RETRY) : null;
        retryEnabled = retryPolicy != null && (Boolean) retryPolicy.get(GatewayConfig.GatewayProperties.GATEWAY_RETRY_ENABLED);
        if (retryEnabled) {
            seconds = Integer.parseInt(retryPolicy.get(GatewayConfig.GatewayProperties.GATEWAY_ATTEMP_SECONDS).toString());
            attemps = Integer.valueOf(retryPolicy.get(GatewayConfig.GatewayProperties.GATEWAY_ATTEMPS).toString());
        }

        baseName = gatewayBaseName;
        postInitActions();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecord(final HandlerContextImpl context) {
        final CRecord record = context.consumedRecord();
        if (isReplay(record)) {
            final TransactionChangelog transactionChangelog = findChangelogByReference(record);
            if (transactionChangelog != null) {
                final T response = parseChangelogFromString(transactionChangelog.getOutput());
                saveChangelogAndProcessOutput(context, response, true);
            }
        } else {
            final T response = attemp(record, 0);
            saveChangelogAndProcessOutput(context, response, false);
        }
    }

    /**
     * Search a changelog by a reference
     *
     * @param record record with reference
     * @return changelog
     */
    protected static TransactionChangelog findChangelogByReference(final CRecord record) {
        return (TransactionChangelog) States.get().getStore(INTERNAL_SUFFIX + KEY_SUFFIX).findById(record.recordHeaders().find(CommandHeaderType.ENTITY_UUID_KEY.getName()).asString());
    }

    private void saveChangelogAndProcessOutput(final HandlerContextImpl context, final T response, final boolean replay) {
        if (!replay) {
            saveChangelog(context, response);
        }
        processResult(context.consumedRecord(), response);
    }

    /**
     * Call attemp
     *
     * @param record    record for call
     * @param attempNum number of attemp
     * @return response
     */
    protected T attemp(final CRecord record, final int attempNum) {
        final T response = call(record);

        if ((response == null || !isSuccess(response)) && retryEnabled && attempNum < attemps) {
            try {
                Thread.sleep(seconds);
            } catch (final InterruptedException e) { //NOSONAR
                logger.error("Problems sleeping the thread", e);
            }
            return attemp(record, attempNum + 1);
        }
        return response;
    }

    /**
     * Call to external component
     *
     * @param record record
     * @return response
     */
    public abstract T call(CRecord record);

    /**
     * Check if the call is success in the response
     *
     * @param response call response
     * @return true/false
     */
    protected abstract Boolean isSuccess(T response);

    /**
     * Parse output string to response
     *
     * @param output json string
     * @return response
     */
    public T parseChangelogFromString(final String output) {
        try {
            final Class classType = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            return (T) om.readValue(output, classType);
        } catch (final IOException | IllegalArgumentException e) {
            logger.error("Cannot parse to string changelog", e);
            return null;
        }
    }

    /**
     * Parse response to string
     *
     * @param response response
     * @return json string
     */
    public String parseChangelogToString(final T response) {
        try {
            return om.writeValueAsString(response);
        } catch (final IOException e) {
            logger.error("Cannot parse to string changelog objects", e);
            return "";
        }
    }

    /**
     * Send event with the original record and response
     *
     * @param originalRecord original record
     * @param outputEvent    response
     * @param <O>            Response type
     */
    protected static <O extends SpecificRecordBase> void sendEvent(final CRecord originalRecord, final O outputEvent) {
        sendEvent(baseName, originalRecord, outputEvent);
    }

    /**
     * Send event to a base name
     *
     * @param eventBaseName  base name
     * @param originalRecord original record
     * @param outputEvent    output
     * @param <O>            Output type
     */
    protected static <O extends SpecificRecordBase> void sendEvent(final String eventBaseName, final CRecord originalRecord, final O outputEvent) {
        boolean replay = false;
        if (originalRecord != null && isReplay(originalRecord)) {
            replay = true;
        }
        final Event.Builder eventBuilder = new Event.Builder(originalRecord, producer, replay)
                .to(eventBaseName).producerName("gateway").value(outputEvent);


        eventBuilder.build().send(GatewayService::handleOutPutted);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void processResult(CRecord originRecord, T result);

    /**
     * Save a changelog with original record and response
     *
     * @param context  original context
     * @param response response
     */
    protected void saveChangelog(final HandlerContextImpl context, final T response) {
        final CRecord originalRecord = context.consumedRecord();
        final String id = UUID.randomUUID().toString();
        final TransactionChangelog outputEvent = new TransactionChangelog(id, originalRecord.value().toString(), parseChangelogToString(response));

        context.repository().create(GatewayAggregate.class, id, outputEvent, GatewayService::handleOutPutted);
    }

    /**
     * Check if the record is in replay
     *
     * @param record record
     * @return true/false
     */
    protected static Boolean isReplay(final CRecord record) {
        return record.recordHeaders() != null && record.isReplayMode();
    }

    /**
     * Manage output response
     *
     * @param o Output
     * @param e Exception
     */
    protected static void handleOutPutted(final Object o, final Exception e) {
        if (e != null) {
            // TODO retry?
        }
    }
}
