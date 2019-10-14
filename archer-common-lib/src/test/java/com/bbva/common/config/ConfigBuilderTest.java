package com.bbva.common.config;

import com.bbva.common.config.annotations.Config;
import com.bbva.common.config.annotations.SecureConfig;
import com.bbva.common.config.util.ConfigurationUtil;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.util.PowermockExtension;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.yaml.snakeyaml.Yaml;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class ConfigBuilderTest {

    @DisplayName("Initialize common configuration")
    @Test
    public void initCommonConfigurationOk() {
        final AppConfig configuration = ConfigBuilder.create();
        final AppConfig appConfig = ConfigBuilder.get();
        Assertions.assertAll("configurations",
                () -> Assertions.assertEquals("", configuration.get().get("schema.registry.url")),
                () -> Assertions.assertEquals("PLAINTEXT://", configuration.get().get("bootstrap.servers")),
                () -> Assertions.assertEquals("1", configuration.get().get("replication.factor")),
                () -> Assertions.assertNotNull(appConfig),
                () -> Assertions.assertEquals(configuration, appConfig)
        );
    }


    @DisplayName("Initialize common setting custom configuration")
    @Test
    public void initCustomConfigurationOk() {

        final Config customConfig = PowerMockito.mock(Config.class);
        PowerMockito.when(customConfig.file()).thenReturn("custom-config.yml");
        final AppConfig configuration = ConfigBuilder.create(customConfig);

        Assertions.assertAll("configurations",
                () -> Assertions.assertEquals("http://localhost:8081", configuration.get().get("schema.registry.url")),
                () -> Assertions.assertEquals("PLAINTEXT://localhost:9092", configuration.get().get("bootstrap.servers")),
                () -> Assertions.assertEquals("3", configuration.get().get("replication.factor"))
        );
    }

    @DisplayName("Initialize secure setting custom configuration")
    @Test
    public void initSecureConfigurationOk() {

        final SecureConfig customConfig = PowerMockito.mock(SecureConfig.class);
        PowerMockito.when(customConfig.file()).thenReturn("secure-config.yml");
        final AppConfig configuration = ConfigBuilder.create(customConfig);

        Assertions.assertAll("configurations",
                () -> Assertions.assertEquals("jas-config", configuration.get().get("sasl.jaas.config")),
                () -> Assertions.assertEquals("SASL://localhost:9093", configuration.get().get("bootstrap.servers")),
                () -> Assertions.assertEquals("SASL_SSL", configuration.get().get("security.protocol"))
        );

    }

    @DisplayName("Initialize secure setting custom configuration")
    @Test
    public void initSecureConfiguration2Ok() {

        final AppConfig configuration = ConfigBuilder.create((SecureConfig) null);

        Assertions.assertAll("configurations",
                () -> Assertions.assertEquals("SASL_SSL", configuration.get().get("security.protocol"))
        );

    }

    @DisplayName("Get not found file config")
    @Test
    public void getConfigFromFileKo() {
        Assertions.assertThrows(ApplicationException.class, () ->
                ConfigurationUtil.getConfigFromFile(new Yaml(), ConfigBuilderTest.class.getClassLoader(), "not-found.yml")
        );
    }
}
