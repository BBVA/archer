package com.bbva.common.utils.headers.types;

import com.bbva.common.utils.headers.HeaderType;

public enum EventHeaderType implements HeaderType {
    PRODUCTOR_NAME_KEY("productor.name"),
    NAME_KEY("name"),
    REFERENCE_ID_KEY("reference.id");

    private final String name;

    EventHeaderType(final String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
}
