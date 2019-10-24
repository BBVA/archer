package com.bbva.ddd.domain.changelogs.repository.aggregates;

import com.bbva.ddd.domain.changelogs.repository.aggregates.callbacks.AggregateCallback;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Aggregate common implementation
 *
 * @param <K> Key class
 * @param <V> Value specific record class
 */
public class CommonAggregate<K, V extends SpecificRecordBase> extends AbstractAggregate<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(CommonAggregate.class);

    /**
     * Constructor
     *
     * @param id   id of the aggregate
     * @param data value
     */
    public CommonAggregate(final K id, final V data) {
        super(id, data);
    }

    /**
     * Update data in the aggregate
     *
     * @param newValue new data to update
     * @param command  command that produce the update
     * @param callback callback for the apply
     */
    public void update(final V newValue, final AggregateCallback callback) {
        apply("update", newValue,
                (id, e) -> onComplete(callback, e, "Update ..."));
    }

    /**
     * Delete record
     *
     * @param command  command that produce the deletion
     * @param callback callback for the apply
     */
    public void delete(final AggregateCallback callback) {
        apply("delete",
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
