package com.bbva.ddd.domain.aggregates.callbacks;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.ProducerCallback;
import org.apache.avro.specific.SpecificRecordBase;

import java.lang.reflect.InvocationTargetException;

public interface DeleteRecordCallback<K, V extends SpecificRecordBase> {

    // public void apply(String method, SpecificRecord PRecord, ProducerCallback callback);
    void apply(String method, Class<V> recordClass, CRecord record, ProducerCallback callback)
            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException;
}
