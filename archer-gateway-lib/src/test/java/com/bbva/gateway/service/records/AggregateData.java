
package com.bbva.gateway.service.records;


import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.common.producers.ProducerCallback;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.callbacks.ApplyRecordCallback;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

public class AggregateData implements AggregateBase {


    @Override
    public SpecificRecordBase getData() {
        return new TransactionChangelog();
    }

    @Override
    public Object getId() {
        return "id";
    }

    @Override
    public void apply(final String method, final SpecificRecordBase value, final CommandRecord commandRecord, final ProducerCallback callback) {

    }

    @Override
    public void setApplyRecordCallback(final ApplyRecordCallback apply) {

    }

    @Override
    public Class<? extends SpecificRecord> getValueClass() {
        return null;
    }
}