package com.bbva.gateway.consumer.builder;


import com.bbva.dataprocessors.builders.dataflows.states.ChangelogBuilder;
import com.bbva.dataprocessors.transformers.EntityTransformer;
import com.bbva.gateway.consumer.transformer.ChangelogTransformer;
import org.apache.avro.specific.SpecificRecordBase;


public class ChangelogKeyBuilder<K, V extends SpecificRecordBase> extends ChangelogBuilder {

    public ChangelogKeyBuilder(final String baseName, final String topic) {
        super(baseName, topic);
    }

    @Override
    protected EntityTransformer newTransformer(final String entityName) {
        return new ChangelogTransformer(entityName);
    }

}
