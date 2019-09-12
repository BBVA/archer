package com.bbva.gateway.service.impl;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.common.consumers.CRecord;
import com.bbva.ddd.domain.AggregateFactory;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.gateway.aggregates.GatewayAggregate;
import com.bbva.gateway.service.IAsyncGatewayService;

public abstract class AsyncGatewayService<T>
        extends GatewayService<T>
        implements IAsyncGatewayService<T> {

    @Override
    public void processRecord(final CRecord record) {

        if (isReplay(record)) {
            final TransactionChangelog transactionChangelog = findChangelogByReference(record);

            if (transactionChangelog != null) {
                final T response = parseChangelogFromString(transactionChangelog.getOutput());
                saveChangelog(record, response, true);
            }
        } else {
            final T response = attemp(record, 0);
            saveChangelog(record, response, false);
        }
    }

    @Override
    protected void saveChangelog(final CRecord originalRecord, final T response, final boolean replayMode) {
        final String id = getId(response);
        final TransactionChangelog outputEvent = new TransactionChangelog(id, originalRecord.value().toString(), parseChangelogToString(response));

        final CommandRecord record = new CommandRecord(originalRecord.topic(), originalRecord.partition(), originalRecord.offset(), originalRecord.timestamp(),
                originalRecord.timestampType(), originalRecord.key(), originalRecord.value(), originalRecord.recordHeaders());

        AggregateFactory.create(GatewayAggregate.class, id, outputEvent, record, GatewayService::handleOutPutted);
        createListener(originalRecord, response);
    }

    public abstract String getId(T response);

    @Override
    public void processResult(final CRecord originRecord, final T result) {

    }

    public static void saveChangelog(final String iden, final String body) {
        final TransactionChangelog changelog = AggregateFactory.load(GatewayAggregate.class, iden).getData();
        if (changelog != null) {
            final TransactionChangelog outputEvent = new TransactionChangelog(iden, changelog.getOutput(), body);

            AggregateFactory.create(GatewayAggregate.class, iden, outputEvent, null, GatewayService::handleOutPutted);
        }
    }
}
