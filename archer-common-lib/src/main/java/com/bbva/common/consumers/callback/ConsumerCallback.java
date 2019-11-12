package com.bbva.common.consumers.callback;

@FunctionalInterface
public interface ConsumerCallback<CRecord, Producer, Boolean> {
    void apply(CRecord record, Producer producer, Boolean isReplay);
}
