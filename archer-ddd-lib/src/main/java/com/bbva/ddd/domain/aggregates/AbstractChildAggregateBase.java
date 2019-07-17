package com.bbva.ddd.domain.aggregates;

import com.bbva.common.producers.ProducerCallback;
import com.bbva.ddd.domain.aggregates.callbacks.ApplyRecordCallback;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

public abstract class AbstractChildAggregateBase<K, V extends SpecificRecordBase> implements AggregateBase<K, V> {

    private final V data;
    private final K id;
    private ApplyRecordCallback applyRecordCallback;

    public AbstractChildAggregateBase(final K id, final V data) {
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
    public void apply(final String method, final V value, final CommandRecord commandRecord,
                      final ProducerCallback callback) {
        applyRecordCallback.apply(method, value, commandRecord, callback);
    }

    @Override
    public final void setApplyRecordCallback(final ApplyRecordCallback apply) {
        this.applyRecordCallback = apply;
    }

    @Override
    public Class<? extends SpecificRecord> getValueClass() {
        return data.getClass();
    }

}
