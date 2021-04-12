package com.bbva.gateway.api;

import com.bbva.common.util.PowermockExtension;
import com.bbva.gateway.GatewayTest;
import com.bbva.gateway.config.ConfigBuilder;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.config.annotations.Config;
import com.bbva.gateway.util.CommandBaseService;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import scala.collection.immutable.HashMap;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({ConfigBuilder.class})
public class CallbackRestTest {

    @DisplayName("Create CallBack Ok")
    @Test
    public void createCallBackOk() {
        final CallbackRest callbackRest = new CallbackRest();
        CallbackRest.init();
        Assertions.assertNotNull(callbackRest);
    }

    @DisplayName("CallBack response Ok")
    @Test
    public void callBackResponseOk() {
        Assertions.assertEquals(Response.Status.OK.getStatusCode(), CallbackRest.callback("request").getStatus());
    }

    @DisplayName("Create CallBack Ok")
    @Test
    public void createCallBackWithoutServicesOk() {
        PowerMockito.mockStatic(ConfigBuilder.class);

        PowerMockito.when(ConfigBuilder.findConfigAnnotation()).thenReturn(PowerMockito.mock(Config.class));

        PowerMockito.when(ConfigBuilder.getServiceClasses(Mockito.anyString())).thenReturn(new ArrayList<>());

        final CallbackRest callbackRest = new CallbackRest();
        CallbackRest.init();
        Assertions.assertNotNull(callbackRest);
    }

    @DisplayName("Create CallBack Ok")
    @Test
    public void createCallBackWithoutActionOk() {
        PowerMockito.mockStatic(ConfigBuilder.class);

        PowerMockito.when(ConfigBuilder.findConfigAnnotation()).thenReturn(GatewayTest.class.getAnnotation(Config.class));
        final List<Class> services = new ArrayList<>();
        services.add(CommandBaseService.class);
        PowerMockito.when(ConfigBuilder.getServiceClasses(Mockito.anyString())).thenReturn(services);

        final LinkedHashMap config = new LinkedHashMap();
        config.put("custom", new HashMap<>());
        final GatewayConfig gatewayConfig = new GatewayConfig();
        gatewayConfig.custom().put("custom", "value");
        PowerMockito.when(ConfigBuilder.create(Mockito.any(Config.class))).thenReturn(gatewayConfig);

        PowerMockito.when(ConfigBuilder.getServiceConfig(Mockito.anyString())).thenReturn(config);

        final CallbackRest callbackRest = new CallbackRest();
        CallbackRest.init();
        Assertions.assertNotNull(callbackRest);
    }

    @DisplayName("Create CallBack Ok")
    @Test
    public void createCallBackCustomProperties() {
        PowerMockito.mockStatic(ConfigBuilder.class);

        PowerMockito.when(ConfigBuilder.findConfigAnnotation()).thenReturn(GatewayTest.class.getAnnotation(Config.class));
        final List<Class> services = new ArrayList<>();
        services.add(CommandBaseService.class);
        PowerMockito.when(ConfigBuilder.getServiceClasses(Mockito.anyString())).thenReturn(services);

        final GatewayConfig gatewayConfig = new GatewayConfig();
        gatewayConfig.custom().put("custom", "value");
        PowerMockito.when(ConfigBuilder.create(Mockito.any(Config.class))).thenReturn(gatewayConfig);

        final LinkedHashMap config = new LinkedHashMap();
        config.put("commandAction", "action");
        PowerMockito.when(ConfigBuilder.getServiceConfig(Mockito.anyString())).thenReturn(config);

        final CallbackRest callbackRest = new CallbackRest();
        CallbackRest.init();
        Assertions.assertNotNull(callbackRest);
    }
}
