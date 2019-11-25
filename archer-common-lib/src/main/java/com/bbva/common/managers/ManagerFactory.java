package com.bbva.common.managers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.callback.ConsumerCallback;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.managers.kafka.KafkaAtLeastOnceManager;
import com.bbva.common.managers.kafka.KafkaExactlyOnceManager;

import java.util.List;

public class ManagerFactory {

    public enum EventStores {
        kafka
    }

    public enum DeliveryTypes {
        exactlyOnce("exactly-once"),
        atLeastOnce("at-least-once");

        private final String value;

        DeliveryTypes(final String s) {
            value = s;
        }

        public static DeliveryTypes valueOfType(final String text) {
            for (final DeliveryTypes e : values()) {
                if (e.value.equals(text)) {
                    return e;
                }
            }
            return null;
        }
    }

    public static Manager create(final String eventStore, final String deliveryType, final List<String> sources, final ConsumerCallback callback, final AppConfig appConfig) {
        final EventStores eventStoreEnum = EventStores.valueOf(eventStore);
        if (eventStoreEnum != null && eventStoreEnum.equals(EventStores.kafka)) {
            final DeliveryTypes deliveryTypeEnum = DeliveryTypes.valueOfType(deliveryType);
            if (deliveryTypeEnum != null) {
                if (deliveryTypeEnum.equals(DeliveryTypes.atLeastOnce)) {
                    return new KafkaAtLeastOnceManager(sources, callback, appConfig);
                } else if (deliveryTypeEnum.equals(DeliveryTypes.exactlyOnce)) {
                    return new KafkaExactlyOnceManager(sources, callback, appConfig);
                }
            } else {
                throw new ApplicationException("Invalid Delivery Type");
            }
        }
        return null;
    }

}
