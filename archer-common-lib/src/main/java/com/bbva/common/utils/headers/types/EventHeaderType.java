package com.bbva.common.utils.headers.types;

import com.bbva.common.utils.headers.HeaderType;

/**
 * Event header types
 */
public enum EventHeaderType implements HeaderType {
    TYPE_VALUE("event"),
    PRODUCER_NAME_KEY("producer.name"),
    NAME_KEY("name"),
    REFERENCE_RECORD_KEY("reference.record");

    private final String name;

    /**
     * Constructor
     *
     * @param name name of type
     */
    EventHeaderType(final String name) {
        this.name = name;
    }

    /**
     * Get type name
     *
     * @return the name
     */
    @Override
    public String getName() {
        return name;
    }
}
