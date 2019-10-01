package com.bbva.common.utils.headers.types;

import com.bbva.common.utils.headers.HeaderType;

/**
 * Changelog header types
 */
public enum ChangelogHeaderType implements HeaderType {
    TYPE_VALUE("changelog"),
    UUID_KEY("uuid"),
    AGGREGATE_UUID_KEY("aggregate.uuid"),
    AGGREGATE_NAME_KEY("aggregate.name"),
    AGGREGATE_METHOD_KEY("aggregate.method");

    private final String name;

    /**
     * Constructor
     *
     * @param name name of type
     */
    ChangelogHeaderType(final String name) {
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
