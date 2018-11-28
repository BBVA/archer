package com.bbva.ddd.domain.aggregates.callbacks;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.ProducerCallback;
import org.apache.avro.specific.SpecificRecord;

public interface ApplyRecordCallback {

    // public void apply(String method, SpecificRecord PRecord, ProducerCallback callback);
    void apply(String method, SpecificRecord record, CRecord message, ProducerCallback callback);
}
