package com.bbva.common.config;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.powermock.api.mockito.PowerMockito;

@RunWith(JUnit4.class)
public class AppConfiguration2Test {

    @Test
    public void initCommonConfigurationOk() {
        final ApplicationConfig configuration = new AppConfiguration().init();

        Assert.assertEquals("", configuration.get().get("schema.registry.url"));
        Assert.assertEquals("PLAINTEXT://", configuration.get().get("bootstrap.servers"));
        Assert.assertEquals("1", configuration.get().get("replication.factor"));
    }


    @Test
    public void initCustomConfigurationOk() {

        final Config customConfig = PowerMockito.mock(Config.class);
        PowerMockito.when(customConfig.file()).thenReturn("custom-config.yml");
        final ApplicationConfig configuration = new AppConfiguration().init(customConfig);

        Assert.assertEquals("http://localhost:8081", configuration.get().get("schema.registry.url"));
        Assert.assertEquals("PLAINTEXT://localhost:9092", configuration.get().get("bootstrap.servers"));
        Assert.assertEquals("3", configuration.get().get("replication.factor"));
    }


    @Test
    public void initSecureConfigurationOk() {

        final SecureConfig customConfig = PowerMockito.mock(SecureConfig.class);
        PowerMockito.when(customConfig.file()).thenReturn("secure-config.yml");
        final ApplicationConfig configuration = new AppConfiguration().init(customConfig);

        Assert.assertEquals("jas-config", configuration.get().get("sasl.jaas.config"));
        Assert.assertEquals("SASL://localhost:9093", configuration.get().get("bootstrap.servers"));
        Assert.assertEquals("SASL_SSL", configuration.get().get("security.protocol"));
    }
}
