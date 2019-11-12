package com.bbva.common.consumers.adapters;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.callback.ConsumerCallback;

import java.util.List;

public class ConsumerAdapterFactory {

    public enum EventStores {
        Kafka
    }

    public static ConsumerAdapter create(final EventStores eventStore, final List<String> sources, final ConsumerCallback callback, final AppConfig appConfig) {
        switch (eventStore) {
            case Kafka:
                return new KafkaConsumerAdapter(sources, callback, appConfig);
        }
        return null;

    }

}
