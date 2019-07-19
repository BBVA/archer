package com.bbva.common.producers.config;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.util.PowermockExtension;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class CachedProducerTest {

    @DisplayName("Initialize common configuration")
    @Test
    public void initCommonConfigurationOk() {
        final ApplicationConfig configuration = new AppConfiguration().init();

        Assertions.assertAll("configurations",
                () -> Assertions.assertEquals("", configuration.get().get("schema.registry.url")),
                () -> Assertions.assertEquals("PLAINTEXT://", configuration.get().get("bootstrap.servers")),
                () -> Assertions.assertEquals("1", configuration.get().get("replication.factor"))
        );
    }

}
