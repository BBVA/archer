package com.bbva.ddd.domain.aggregates;

import com.bbva.ddd.domain.aggregates.callbacks.AggregateCallback;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;

public class SpecificAggregate<K, V extends SpecificRecordBase> extends AbstractAggregateBase<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(SpecificAggregate.class);

    public SpecificAggregate(final K id, final V data) {
        super(id, data);
    }

    public void update(final V newValue, final CommandRecord command, final AggregateCallback callback) {
        this.apply("update", newValue,
                transformUpdateRecord(command),
                (id, e) -> onComplete(callback, e, "Update ..."));
    }

    protected static CommandRecord transformUpdateRecord(final CommandRecord command) {
        return command;
    }

    public void delete(final CommandRecord command, final AggregateCallback callback) {
        this.apply("delete", command,
                (id, e) -> onComplete(callback, e, "Delete ..."));
    }

    protected static void onComplete(final AggregateCallback callback, final Exception e, final String message) {
        if (e != null) {
            logger.error("Error performing the action", e);
        } else {
            logger.info(message);
            if (callback != null) {
                callback.onComplete();
            }
        }
    }
}
