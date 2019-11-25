package com.bbva.gateway;

import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.TopicManager;
import com.bbva.dataprocessors.DataProcessor;
import com.bbva.ddd.domain.Domain;
import com.bbva.gateway.config.annotations.Config;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@Config(file = "gateway.yml")
@ExtendWith(PowermockExtension.class)
@PrepareForTest({Domain.class, TopicManager.class, DataProcessor.class})
public class GatewayTest {

    @DisplayName("Create gateway")
    @Test
    public void createGatewayOk() throws Exception {
        PowerMockito.whenNew(Domain.class).withAnyArguments().thenReturn(PowerMockito.mock(Domain.class));
        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));

        PowerMockito.mockStatic(TopicManager.class);

        final GatewayDomain domain = GatewayDomain.Builder.create().build();

        Assertions.assertNotNull(domain);
    }

    @DisplayName("Configure gateway with specific class")
    @Test
    public void configureGatewayOk() throws Exception {
        PowerMockito.whenNew(Domain.class).withAnyArguments().thenReturn(PowerMockito.mock(Domain.class));
        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));

        PowerMockito.mockStatic(TopicManager.class);
        
        final GatewayDomain domain = GatewayDomain.Builder.create(GatewayTest.class).build();

        Assertions.assertNotNull(domain);
    }

}
