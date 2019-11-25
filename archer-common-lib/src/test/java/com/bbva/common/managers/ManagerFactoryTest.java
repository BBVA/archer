package com.bbva.common.managers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.util.PowermockExtension;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

import java.util.ArrayList;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class ManagerFactoryTest {

    @DisplayName("Create KafkaExactlyOnceManager ok")
    @Test
    public void createKafkaExactlyOnceManagerOk() {
        final AppConfig configuration = ConfigBuilder.create();
        final Manager manager = ManagerFactory.create("kafka", "exactly-once", new ArrayList<>(), null, configuration);

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(manager)
        );
    }

    @DisplayName("Create KafkaAtLeastOnceManager ok")
    @Test
    public void createKafkaAtLeastOnceManagerOk() {
        final AppConfig configuration = ConfigBuilder.create();
        final Manager manager = ManagerFactory.create("kafka", "at-least-once", new ArrayList<>(), null, configuration);

        Assertions.assertAll("KafkaAtLeastOnceManager",
                () -> Assertions.assertNotNull(manager)
        );
    }

    @DisplayName("Create Kafka invalid delivery")
    @Test
    public void createKafkaInvalidDeliveryManagerOk() {
        Assertions.assertThrows(ApplicationException.class, () -> {
            final AppConfig configuration = ConfigBuilder.create();
            ManagerFactory.create("kafka", "invalid", new ArrayList<>(), null, configuration);
        });
    }

}
