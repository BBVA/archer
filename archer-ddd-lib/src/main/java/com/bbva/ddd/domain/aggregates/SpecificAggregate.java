package com.bbva.ddd.domain.aggregates;

import com.bbva.ddd.domain.aggregates.callbacks.AggregateCallback;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Specific Aggregate common implementation
 *
 * @param <K> Key class
 * @param <V> Value specific record class
 */
public class SpecificAggregate<K, V extends SpecificRecordBase> extends AbstractAggregateBase<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(SpecificAggregate.class);

    /**
     * Constructor
     *
     * @param id   id of the aggregate
     * @param data value
     */
    public SpecificAggregate(final K id, final V data) {
        super(id, data);
    }

    /**
     * Update data in the aggregate
     *
     * @param newValue new data to update
     * @param command  command that produce the update
     * @param callback callback for the apply
     */
    public void update(final V newValue, final CommandRecord command, final AggregateCallback callback) {
        apply("update", newValue,
                transformUpdateRecord(command),
                (id, e) -> onComplete(callback, e, "Update ..."));
    }

    /**
     * Delete record
     *
     * @param command  command that produce the deletion
     * @param callback callback for the apply
     */
    public void delete(final CommandRecord command, final AggregateCallback callback) {
        apply("delete", command,
                (id, e) -> onComplete(callback, e, "Delete ..."));
    }

    protected static CommandRecord transformUpdateRecord(final CommandRecord command) {
        return command;
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
