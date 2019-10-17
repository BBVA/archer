package com.bbva.gateway.service.impl;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.handlers.HandlerContextImpl;
import com.bbva.gateway.aggregates.GatewayAggregate;
import com.bbva.gateway.service.IAsyncGatewayService;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;

/**
 * Asynchronous gateway service implementation
 *
 * @param <T> Result type
 */
public abstract class AsyncGatewayService<T>
        extends GatewayService<T>
        implements IAsyncGatewayService<T> {

    private static final Map<String, Repository> respositoryCached = new HashMap<>();

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
                saveChangelog(context, response);
            }
        } else {
            final T response = attemp(record, 0);
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
    public T parseChangelogFromString(final String output) {
        try {
            final Class classType = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            return (T) om.readValue(output, classType);
        } catch (final IOException | IllegalArgumentException e) {
            return null;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveChangelog(final HandlerContextImpl context, final T response) {
        final CRecord originalRecord = context.consumedRecord();
        final String id = getId(response);
        respositoryCached.put(id, context.repository());
        final TransactionChangelog outputEvent = new TransactionChangelog(id, originalRecord.value().toString(), parseChangelogToString(response));

        context.repository().create(GatewayAggregate.class, id, outputEvent, GatewayService::handleOutPutted);

        createListener(originalRecord, response);
    }

    /**
     * Return the id from response
     *
     * @param response response
     * @return id
     */
    public abstract String getId(T response);

    @Override
    public void processResult(final CRecord originRecord, final T result) {

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

                callRepository.create(GatewayAggregate.class, iden, outputEvent, GatewayService::handleOutPutted);
            }
            respositoryCached.remove(iden);
        }

    }
}
