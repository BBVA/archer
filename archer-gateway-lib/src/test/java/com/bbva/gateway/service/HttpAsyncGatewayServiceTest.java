package com.bbva.gateway.service;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.ddd.domain.AggregateFactory;
import com.bbva.ddd.domain.HelperDomain;
import com.bbva.gateway.GatewayTest;
import com.bbva.gateway.config.Configuration;
import com.bbva.gateway.config.annotations.Config;
import com.bbva.gateway.http.RetrofitClient;
import com.bbva.gateway.service.impl.GatewayService;
import com.bbva.gateway.service.impl.HttpAsyncGatewayServiceImpl;
import com.bbva.gateway.service.records.PersonalData;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import retrofit2.Response;

import java.util.Date;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PowerMockIgnore("javax.net.ssl.*")
@PrepareForTest({AggregateFactory.class, HelperDomain.class, GatewayService.class, Response.class, RetrofitClient.class})
public class HttpAsyncGatewayServiceTest {

    @DisplayName("Create service ok")
    @Test
    public void startRestOk() {
        final IAsyncGatewayService service = new HttpAsyncGatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);

        service.init(new Configuration().init(configAnnotation), "baseName");
        service.postInitActions();

        Assertions.assertAll("GatewayService",
                () -> Assertions.assertNotNull(service)
        );
    }

    @DisplayName("Service call ok")
    @Test
    public void callOk() {
        PowerMockito.mockStatic(RetrofitClient.class);
        final Response response = PowerMockito.mock(Response.class);
        PowerMockito.when(RetrofitClient.call(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(response);

        final IGatewayService service = new HttpAsyncGatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(new Configuration().init(configAnnotation), "baseName");

        final Object callResult = ((HttpAsyncGatewayServiceImpl) service).call(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), new RecordHeaders()));

        Assertions.assertAll("GatewayService",
                () -> Assertions.assertNotNull(service),
                () -> Assertions.assertNotNull(callResult)
        );
    }
}

