package com.bbva.ddd.domain.aggregates;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.ProducerCallback;
import com.bbva.ddd.domain.aggregates.callbacks.ApplyRecordCallback;
import com.bbva.ddd.domain.aggregates.callbacks.DeleteRecordCallback;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Common implementation of aggregates
 *
 * @param <K> Key class
 * @param <V> Value class
 */
public abstract class AbstractAggregateBase<K, V extends SpecificRecordBase> implements AggregateBase<K, V> {

    private final V data;
    private final K id;
    private ApplyRecordCallback applyRecordCallback;
    private DeleteRecordCallback<K, V> deleteRecordCallback;

    /**
     * Constructor
     *
     * @param id   id of aggregate
     * @param data data of aggregate
     */
    AbstractAggregateBase(final K id, final V data) {
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
     * @param method        Name of the method which apply new data
     * @param value         New value to apply
     * @param commandRecord Command record which has triggered the domain logic
     * @param callback      Callback executed when message is stored in changelog
     */
    @Override
    public void apply(final String method, final V value, final CommandRecord commandRecord,
                      final ProducerCallback callback) {
        applyRecordCallback.apply(method, value, commandRecord, callback);
    }

    /**
     * Apply new data in changelog
     *
     * @param method   Name of the method which apply new data
     * @param record   New value to apply
     * @param callback Callback executed when message is stored in changelog
     */
    public void apply(final String method, final CRecord record, final ProducerCallback callback) {
        deleteRecordCallback.apply(method, (Class<V>) data.getClass(), record, callback);
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
