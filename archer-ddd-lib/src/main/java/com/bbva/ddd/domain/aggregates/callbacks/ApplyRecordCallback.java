package com.bbva.ddd.domain.aggregates.callbacks;

import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.ProducerCallback;
import org.apache.avro.specific.SpecificRecordBase;

public interface ApplyRecordCallback {

    void apply(String method, SpecificRecordBase value, CRecord referenceRecord, ProducerCallback callback);
}
