package com.bbva.common.utils.headers.types;

import com.bbva.common.utils.headers.HeaderType;

public enum CommonHeaderType implements HeaderType {
    TYPE_KEY("type"),
    FLAG_REPLAY_KEY("flag.replay");

    private final String name;

    CommonHeaderType(final String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
}
