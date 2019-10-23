package com.bbva.ddd.domain.changelogs.repository.aggregates;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.ddd.domain.changelogs.repository.aggregates.callbacks.ApplyRecordCallback;
import com.bbva.ddd.domain.changelogs.repository.aggregates.callbacks.DeleteRecordCallback;
import com.bbva.ddd.domain.commands.consumers.CommandRecord;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Abstract base implementation of aggregates
 *
 * @param <K> Key class
 * @param <V> Value class
 */
public abstract class AbstractAggregate<K, V extends SpecificRecordBase> implements AggregateBase<K, V> {

    private final V data;
    private final K id;
    private ApplyRecordCallback applyRecordCallback;
    private DeleteRecordCallback deleteRecordCallback;

    /**
     * Constructor
     *
     * @param id   id of aggregate
     * @param data data of aggregate
     */
    AbstractAggregate(final K id, final V data) {
        this.id = id;
        this.data = data;
    }

    /**
     * Get current status of the entity in changelog
     *
     * @return the value
     */
    @Override
    public final V getData() {
        return data;
    }

    /**
     * Get entity identifier
     *
     * @return the key
     */
    @Override
    public final K getId() {
        return id;
    }

    /**
     * Apply new data in changelog
     *
     * @param method          Name of the method which apply new data
     * @param value           New value to apply
     * @param referenceRecord Command record which has triggered the domain logic
     * @param callback        Callback executed when message is stored in changelog
     */
    @Override
    public void apply(final String method, final V value, final CommandRecord referenceRecord,
                      final ProducerCallback callback) {
        applyRecordCallback.apply(method, value, referenceRecord, callback);
    }

    /**
     * Apply new data in changelog
     *
     * @param method          Name of the method which apply new data
     * @param referenceRecord New value to apply
     * @param callback        Callback executed when message is stored in changelog
     */
    public void apply(final String method, final CRecord referenceRecord, final ProducerCallback callback) {
        deleteRecordCallback.apply(method, referenceRecord, callback);
    }

    /**
     * Internal method. Not for users
     *
     * @param apply a callback
     */
    @Override
    public final void setApplyRecordCallback(final ApplyRecordCallback apply) {
        applyRecordCallback = apply;
    }

    /**
     * Internal method. Not for users
     *
     * @param apply a callback
     */
    public final void setDeleteRecordCallback(final DeleteRecordCallback apply) {
        deleteRecordCallback = apply;
    }

    /**
     * Get class of the value
     *
     * @return Class of the value
     */
    @Override
    public Class<? extends SpecificRecordBase> getValueClass() {
        return data.getClass();
    }

}
