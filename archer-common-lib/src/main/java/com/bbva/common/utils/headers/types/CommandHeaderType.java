package com.bbva.common.utils.headers.types;

import com.bbva.common.utils.headers.HeaderType;

/**
 * Command header types
 */
public enum CommandHeaderType implements HeaderType {
    TYPE_VALUE("command"),
    UUID_KEY("uuid"),
    NAME_KEY("name"),
    ENTITY_UUID_KEY("entity.uuid");

    private final String name;

    /**
     * Constructor
     *
     * @param name name of type
     */
    CommandHeaderType(final String name) {
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
