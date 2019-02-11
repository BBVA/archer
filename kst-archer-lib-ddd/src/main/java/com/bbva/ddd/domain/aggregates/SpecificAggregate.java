package com.bbva.ddd.domain.aggregates;

import com.bbva.ddd.domain.aggregates.callbacks.AggregateCallback;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import kst.logging.Logger;
import kst.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;

import java.lang.reflect.InvocationTargetException;

public class SpecificAggregate<T, V extends SpecificRecordBase> extends AbstractAggregateBase<T, V> {

    private static final Logger logger = LoggerFactory.getLogger(SpecificAggregate.class);

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
            logger.error("Error performing the action", e);
        } else {
            logger.info(message);
            callback.onComplete();
        }
    }
}
