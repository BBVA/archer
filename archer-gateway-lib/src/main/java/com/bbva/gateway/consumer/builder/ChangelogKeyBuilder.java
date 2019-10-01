package com.bbva.gateway.consumer.builder;


import com.bbva.dataprocessors.builders.dataflows.states.ChangelogBuilder;
import com.bbva.dataprocessors.transformers.EntityTransformer;
import com.bbva.gateway.consumer.transformer.ChangelogTransformer;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Builder to manage entity of changelog
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class ChangelogKeyBuilder<K, V extends SpecificRecordBase> extends ChangelogBuilder {

    /**
     * Constructor
     *
     * @param baseName changelog base name
     * @param topic    topic to read
     */
    public ChangelogKeyBuilder(final String baseName, final String topic) {
        super(baseName, topic);
    }

    /**
     * Set entity transformer
     *
     * @param entityName name of the entity
     * @return transformer instance
     */
    @Override
    protected EntityTransformer newTransformer(final String entityName) {
        return new ChangelogTransformer(entityName);
    }

}
