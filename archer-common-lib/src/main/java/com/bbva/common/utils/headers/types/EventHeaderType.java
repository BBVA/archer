package com.bbva.common.utils.headers.types;

import com.bbva.common.utils.headers.HeaderType;

public enum EventHeaderType implements HeaderType {
    TYPE_VALUE("event"),
    PRODUCER_NAME_KEY("producer.name"),
    NAME_KEY("name"),
    REFERENCE_RECORD_KEY("reference.record");

    private final String name;

    EventHeaderType(final String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
}
