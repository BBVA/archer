package com.bbva.gateway.builders.dataflows.states;


import com.bbva.dataprocessors.builders.dataflows.states.ChangelogBuilder;
import com.bbva.dataprocessors.transformers.EntityTransformer;
import com.bbva.gateway.transformers.HeaderAsKeyStateTransformer;

/**
 * Builder to manage entity of changelog
 */
public class HeaderAsKeyStateBuilder extends ChangelogBuilder {

    /**
     * Constructor
     *
     * @param baseName changelog base name
     * @param topic    topic to read
     */
    public HeaderAsKeyStateBuilder(final String baseName, final String topic) {
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
        return new HeaderAsKeyStateTransformer(entityName);
    }

}
