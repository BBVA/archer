package com.bbva.common.producers;

public interface ProducerCallback {

    void onCompletion(Object id, Exception exception);
}
