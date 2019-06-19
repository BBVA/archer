package com.bbva.common.utils.headers.types;

import com.bbva.common.utils.headers.HeaderType;

public enum ChangelogHeaderType implements HeaderType {
    TYPE_CHANGELOG_VALUE("changelog"),
    UUID_KEY("uuid"),
    TRIGGER_REFERENCE_KEY("trigger.reference"),
    AGGREGATE_UUID_KEY("aggregate.uuid"),
    AGGREGATE_NAME_KEY("aggregate.name"),
    AGGREGATE_METHOD_KEY("aggregate.method");

    private final String name;

    ChangelogHeaderType(final String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
}
