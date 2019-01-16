package com.bbva.ddd.domain.aggregates;

import com.bbva.ddd.domain.aggregates.callbacks.AggregateCallback;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.avro.specific.SpecificRecordBase;

import java.lang.reflect.InvocationTargetException;

public class SpecificAggregate<T, V extends SpecificRecordBase> extends AbstractAggregateBase<T, V> {

    private static final LoggerGen logger = LoggerGenesis.getLogger(SpecificAggregate.class.getName());

    public SpecificAggregate(final T id, final V record) {
        super(id, record);
    }

    public void update(final V modifiedRecord, final CommandRecord command, final AggregateCallback callback) {
        this.apply(
                "update",
                modifiedRecord,
                transformUpdateRecord(command),
                (id, e) -> onComplete(callback, e, "Update ...", "update"));
    }

    protected static CommandRecord transformUpdateRecord(final CommandRecord command) {
        return command;
    }

    public void delete(final CommandRecord command, final AggregateCallback callback)
            throws InvocationTargetException, NoSuchMethodException, InstantiationException,
            IllegalAccessException {
        this.apply(
                "delete",
                command,
                (id, e) -> onComplete(callback, e, "Delete ...", "delete"));
    }

    protected static void onComplete(final AggregateCallback callback, final Exception e, final String message, final String method) {
        if (e != null) {
            logger.error(e);
        } else {
            logger.info(message, method, null);
            callback.onComplete();
        }
    }
}
