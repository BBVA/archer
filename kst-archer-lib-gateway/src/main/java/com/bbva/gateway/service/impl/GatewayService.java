package com.bbva.gateway.service.impl;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.common.consumers.CRecord;
import com.bbva.ddd.HelperDomain;
import com.bbva.ddd.domain.AggregateFactory;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.util.StoreUtil;
import com.bbva.gateway.aggregates.GatewayAggregate;
import com.bbva.gateway.config.Configuration;
import com.bbva.gateway.service.IGatewayService;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.bbva.gateway.constants.ConfigConstants.*;
import static com.bbva.gateway.constants.Constants.*;

public abstract class GatewayService<T>
        implements IGatewayService<T> {
    private static final LoggerGen logger = LoggerGenesis.getLogger(GatewayService.class.getName());

    protected Configuration config;
    protected static ObjectMapper om = new ObjectMapper();
    private LinkedHashMap<String, Object> retryPolicy;
    private Boolean retryEnabled = false;
    private int seconds;
    private int attemps;
    protected static String baseName;

    @Override
    public void init(final Configuration configuration, final String gatewayBaseName) {
        config = configuration;

        retryPolicy = config.getGateway().get(GATEWAY_RETRY) != null ? (LinkedHashMap<String, Object>) config.getGateway().get(GATEWAY_RETRY) : null;
        retryEnabled = retryPolicy != null && (Boolean) retryPolicy.get(GATEWAY_RETRY_ENABLED);
        if (retryEnabled) {
            seconds = Integer.parseInt(retryPolicy.get(GATEWAY_ATTEMP_SECONDS).toString());
            attemps = Integer.valueOf(retryPolicy.get(GATEWAY_ATTEMPS).toString());
        }

        baseName = gatewayBaseName;
        postInitActions();

    }

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

    protected static TransactionChangelog findChangelogByReference(final CRecord record) {
        return (TransactionChangelog) StoreUtil.getStore(INTERNAL_SUFFIX + KEY_SUFFIX).findById(record.recordHeaders().find(HEADER_REFERENCE_ID).asString());
    }

    private void saveChangelogAndProcessOutput(final CRecord record, final T response, final boolean replay) {
        if (!replay) {
            saveChangelog(record, response, replay);
        }
        processResult(record, response);
    }

    protected T attemp(final CRecord record, final int attempNum) {
        final T response = call(record);

        if ((response == null || !isSuccess(response)) && retryEnabled && attempNum < attemps) {
            try {
                Thread.sleep(seconds);
            } catch (final InterruptedException e) {
                logger.error("Problems sleeping the thread", e);
            }
            return attemp(record, attempNum + 1);
        }
        return response;
    }

    public abstract T call(CRecord record);

    protected abstract Boolean isSuccess(T response);

    public T parseChangelogFromString(final String output) {
        try {
            return om.readValue(output, new TypeReference() {
            });
        } catch (final IOException e) {
            logger.error("Cannot parse to string changelog", e);
            return null;
        }
    }

    public String parseChangelogToString(final T response) {
        try {
            return om.writeValueAsString(response);
        } catch (final IOException e) {
            logger.error("Cannot parse to string changelog objects", e);
            return "";
        }
    }

    protected static <O extends SpecificRecord> void sendEvent(final CRecord originalRecord, final O outputEvent) {
        try {
            if (originalRecord != null) {
                HelperDomain.get().sendEventLogTo(baseName).send("gateway", outputEvent, GatewayService::handleOutPutted, isReplay(originalRecord), originalRecord.key());
            } else {
                HelperDomain.get().sendEventLogTo(baseName).send("gateway", outputEvent, GatewayService::handleOutPutted);
            }
        } catch (final InterruptedException | ExecutionException e) {
            logger.error("Problems generating event", e);
        }
    }

    @Override
    public abstract void processResult(CRecord originRecord, T result);

    protected void saveChangelog(final CRecord originRecord, final T response, final boolean replayMode) {
        final String id = UUID.randomUUID().toString();
        final TransactionChangelog outputEvent = new TransactionChangelog(id, originRecord.value().toString(), parseChangelogToString(response));

        final CommandRecord record = new CommandRecord(originRecord.topic(), originRecord.partition(), originRecord.offset(), originRecord.timestamp(),
                originRecord.timestampType(), originRecord.key(), originRecord.value(), originRecord.recordHeaders());

        AggregateFactory.create(GatewayAggregate.class, id, outputEvent, record, GatewayService::handleOutPutted);
    }

    protected static Boolean isReplay(final CRecord record) {
        final Boolean replayValue = record.recordHeaders() != null
                ? record.isReplayMode() : false;
        return replayValue;
    }

    protected static void handleOutPutted(final Object o, final Exception e) {
        if (e != null) {
            // TODO retry?
        }
    }
}
