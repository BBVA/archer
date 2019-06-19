package com.bbva.common.utils.headers.types;

import com.bbva.common.utils.headers.HeaderType;

public enum CommandHeaderType implements HeaderType {
    UUID_KEY("uuid"),
    NAME_KEY("name"),
    ENTITY_ID_KEY("entity.id");

    private final String name;

    CommandHeaderType(final String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
}
