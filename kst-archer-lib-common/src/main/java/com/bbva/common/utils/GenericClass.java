package com.bbva.common.utils;

public class GenericClass<T> {

    private final Class<T> type;

    public GenericClass(Class<T> type) {
        this.type = type;
    }

    public Class<T> getType() {
        return this.type;
    }
}