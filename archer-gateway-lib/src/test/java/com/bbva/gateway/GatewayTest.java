package com.bbva.gateway;

import com.bbva.common.util.PowermockExtension;
import com.bbva.ddd.domain.DomainBuilder;
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
@PrepareForTest({DomainBuilder.class})
public class GatewayTest {

    @DisplayName("Create gateway")
    @Test
    public void createGatewayOk() throws Exception {
        PowerMockito.whenNew(DomainBuilder.class).withAnyArguments().thenReturn(PowerMockito.mock(DomainBuilder.class));

        final Gateway gateway = new Gateway();
        gateway.configure();
        gateway.init();

        Assertions.assertNotNull(gateway);
    }

    @DisplayName("Configure gateway with specific class")
    @Test
    public void configureGatewayOk() throws Exception {
        PowerMockito.whenNew(DomainBuilder.class).withAnyArguments().thenReturn(PowerMockito.mock(DomainBuilder.class));

        final Gateway gateway = new Gateway();
        gateway.configure(GatewayTest.class);
        gateway.init();

        Assertions.assertNotNull(gateway);
    }

}
