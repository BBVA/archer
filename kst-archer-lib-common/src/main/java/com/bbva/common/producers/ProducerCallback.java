package com.bbva.common.producers;

public interface ProducerCallback {

    public void onCompletion(Object id, Exception exception);
}
