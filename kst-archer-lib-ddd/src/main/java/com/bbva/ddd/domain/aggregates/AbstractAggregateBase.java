package com.bbva.ddd.domain.aggregates;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.ProducerCallback;
import com.bbva.ddd.domain.aggregates.callbacks.ApplyRecordCallback;
import com.bbva.ddd.domain.aggregates.callbacks.DeleteRecordCallback;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.avro.specific.SpecificRecordBase;

import java.lang.reflect.InvocationTargetException;

public abstract class AbstractAggregateBase<K, V extends SpecificRecordBase> implements AggregateBase<K, V> {

    private final V data;
    private final K id;
    private ApplyRecordCallback applyRecordCallback;
    private DeleteRecordCallback<K, V> deleteRecordCallback;

    public AbstractAggregateBase(final K id, final V data) {
        this.id = id;
        this.data = data;
    }

    @Override
    public final V getData() {
        return data;
    }

    @Override
    public final K getId() {
        return id;
    }

    @Override
    public void apply(final String method, final V record, final CommandRecord commandMessage,
            final ProducerCallback callback) {
        applyRecordCallback.apply(method, record, commandMessage, callback);
    }

    public void apply(final String method, final CRecord record, final ProducerCallback callback)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        deleteRecordCallback.apply(method, (Class<V>) data.getClass(), record, callback);
    }

    @Override
    public final void setApplyRecordCallback(final ApplyRecordCallback apply) {
        this.applyRecordCallback = apply;
    }

    public final void setDeleteRecordCallback(final DeleteRecordCallback apply) {
        this.deleteRecordCallback = apply;
    }

    @Override
    public Class<? extends SpecificRecordBase> getValueClass() {
        return data.getClass();
    }

}
