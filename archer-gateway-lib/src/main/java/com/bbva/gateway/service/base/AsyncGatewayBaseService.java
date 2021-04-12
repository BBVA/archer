package com.bbva.gateway.service.base;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.handlers.contexts.HandlerContext;
import com.bbva.gateway.aggregates.GatewayAggregate;
import com.bbva.gateway.service.AsyncGatewayService;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;

/**
 * Asynchronous gateway service implementation
 *
 * @param <R> Result type
 */
public abstract class AsyncGatewayBaseService<R> extends GatewayBaseService<R> implements AsyncGatewayService<R> {

    private static final Logger logger = LoggerFactory.getLogger(GatewayBaseService.class);

    private static final Map<String, Repository> respositoryCached = new HashMap<>();

    /**
     * Return the transaction identifier from the response.
     *
     * @param response response of the request
     * @return transaction identifier
     */
    public abstract String transactionId(R response);

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
        }
    }

    /**
     * Parse output string to response
     *
     * @param output json string
     * @return response
     */
    @Override
    public R parseChangelogFromString(final String output) {
        try {
            final Class classType = (Class<R>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            return (R) om.readValue(output, classType);
        } catch (final IOException | IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveChangelog(final HandlerContext context, final R response) {
        final CRecord originalRecord = context.consumedRecord();
        final String id = transactionId(response);
        respositoryCached.put(id, context.repository());
        final TransactionChangelog outputEvent = new TransactionChangelog(id, originalRecord.value().toString(), parseChangelogToString(response));

        context.repository().create(GatewayAggregate.class, id, outputEvent, (key, e) -> {

        });

        createListener(originalRecord, response);
    }

    @Override
    public void processResponse(final HandlerContext context, final R result) {

    }

    /**
     * Save the body by id in the changelog
     *
     * @param iden id
     * @param body body
     */
    public static void saveChangelog(final String iden, final String body) {
        final Repository callRepository = respositoryCached.get(iden);
        if (callRepository != null) {
            final TransactionChangelog changelog = callRepository.load(GatewayAggregate.class, iden).getData();
            if (changelog != null) {
                final TransactionChangelog outputEvent = new TransactionChangelog(iden, changelog.getOutput(), body);

                callRepository.create(GatewayAggregate.class, iden, outputEvent, (key, e) -> {
                    if (e != null) {
                        logger.error("Error adding changelog", e);
                    } else {
                        logger.info("Response has been saved in event store as a transaction changelog with key {}", key);
                    }
                });
            }
            respositoryCached.remove(iden);
        }

    }
}
