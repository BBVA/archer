package com.bbva.common.utils.headers.types;

import com.bbva.common.utils.headers.HeaderType;

public enum CommonHeaderType implements HeaderType {
    TYPE_KEY("type"),
    FLAG_REPLAY_KEY("flag.replay"),
    REFERENCE_RECORD_TYPE_KEY("reference.record.type"),
    REFERENCE_RECORD_KEY_KEY("reference.record.key"),
    REFERENCE_RECORD_POSITION_KEY("reference.record.position");

    private final String name;

    CommonHeaderType(final String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
}
