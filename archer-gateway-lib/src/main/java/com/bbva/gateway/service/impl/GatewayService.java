package com.bbva.gateway.service.impl;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.ddd.domain.AggregateFactory;
import com.bbva.ddd.domain.HelperDomain;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.util.StoreUtil;
import com.bbva.gateway.aggregates.GatewayAggregate;
import com.bbva.gateway.config.Configuration;
import com.bbva.gateway.service.IGatewayService;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.UUID;

import static com.bbva.gateway.constants.ConfigConstants.*;
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

    protected Configuration config;
    protected static ObjectMapper om = new ObjectMapper();
    private Boolean retryEnabled = false;
    private int seconds;
    private int attemps;
    protected static String baseName;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final Configuration configuration, final String gatewayBaseName) {
        config = configuration;

        final LinkedHashMap<String, Object> retryPolicy = config.getGateway().get(GATEWAY_RETRY) != null ? (LinkedHashMap<String, Object>) config.getGateway().get(GATEWAY_RETRY) : null;
        retryEnabled = retryPolicy != null && (Boolean) retryPolicy.get(GATEWAY_RETRY_ENABLED);
        if (retryEnabled) {
            seconds = Integer.parseInt(retryPolicy.get(GATEWAY_ATTEMP_SECONDS).toString());
            attemps = Integer.valueOf(retryPolicy.get(GATEWAY_ATTEMPS).toString());
        }

        baseName = gatewayBaseName;
        postInitActions();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecord(final CRecord record) {
        if (isReplay(record)) {
            final TransactionChangelog transactionChangelog = findChangelogByReference(record);
            if (transactionChangelog != null) {
                final T response = parseChangelogFromString(transactionChangelog.getOutput());
                saveChangelogAndProcessOutput(record, response, true);
            }
        } else {
            final T response = attemp(record, 0);
            saveChangelogAndProcessOutput(record, response, false);
        }
    }

    /**
     * Search a changelog by a reference
     *
     * @param record record with reference
     * @return changelog
     */
    protected static TransactionChangelog findChangelogByReference(final CRecord record) {
        return (TransactionChangelog) StoreUtil.getStore(INTERNAL_SUFFIX + KEY_SUFFIX).findById(record.recordHeaders().find(CommonHeaderType.REFERENCE_RECORD_KEY_KEY.getName()).asString());
    }

    private void saveChangelogAndProcessOutput(final CRecord record, final T response, final boolean replay) {
        if (!replay) {
            saveChangelog(record, response, false);
        }
        processResult(record, response);
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
            return om.readValue(output, new TypeReference() {
            });
        } catch (final IOException e) {
            logger.error("Cannot parse to string changelog", e);
            return null;
        }
    }

    /**
     * Parse response to string
     *
     * @param response response
     * @return json sring
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
     * Send evet with the original record and response
     *
     * @param originalRecord original record
     * @param outputEvent    response
     * @param <O>            Response type
     */
    protected static <O extends SpecificRecord> void sendEvent(final CRecord originalRecord, final O outputEvent) {
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
    protected static <O extends SpecificRecord> void sendEvent(final String eventBaseName, final CRecord originalRecord, final O outputEvent) {

        if (originalRecord != null) {
            HelperDomain.get().sendEventTo(eventBaseName).send("gateway", outputEvent, isReplay(originalRecord), originalRecord, GatewayService::handleOutPutted);
        } else {
            HelperDomain.get().sendEventTo(eventBaseName).send("gateway", outputEvent, GatewayService::handleOutPutted);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void processResult(CRecord originRecord, T result);

    /**
     * Save a changelog with original record and response
     *
     * @param originRecord original record
     * @param response     response
     * @param replayMode   true/false
     */
    protected void saveChangelog(final CRecord originRecord, final T response, final boolean replayMode) {
        final String id = UUID.randomUUID().toString();
        final TransactionChangelog outputEvent = new TransactionChangelog(id, originRecord.value().toString(), parseChangelogToString(response));

        final CommandRecord record = new CommandRecord(originRecord.topic(), originRecord.partition(), originRecord.offset(), originRecord.timestamp(),
                originRecord.timestampType(), originRecord.key(), originRecord.value(), originRecord.recordHeaders());

        AggregateFactory.create(GatewayAggregate.class, id, outputEvent, record, GatewayService::handleOutPutted);
    }

    /**
     * Check if the recprd is in replay
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
