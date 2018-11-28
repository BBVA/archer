package com.bbva.ddd.domain.aggregates;

import com.bbva.common.producers.ProducerCallback;
import com.bbva.ddd.domain.aggregates.callbacks.ApplyRecordCallback;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

public interface AggregateBase<K, V extends SpecificRecordBase> {

    V getData();

    K getId();

    void apply(String method, V record, CommandRecord commandMessage, ProducerCallback callback);

    void setApplyRecordCallback(ApplyRecordCallback apply);

    Class<? extends SpecificRecord> getValueClass();
}
