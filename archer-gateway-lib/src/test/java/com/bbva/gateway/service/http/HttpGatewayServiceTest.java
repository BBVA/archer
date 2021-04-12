package com.bbva.gateway.service.http;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.ddd.domain.changelogs.repository.RepositoryImpl;
import com.bbva.ddd.domain.events.producers.Event;
import com.bbva.ddd.domain.handlers.contexts.HandlerContextImpl;
import com.bbva.gateway.GatewayTest;
import com.bbva.gateway.config.ConfigBuilder;
import com.bbva.gateway.config.annotations.Config;
import com.bbva.gateway.http.RetrofitClient;
import com.bbva.gateway.http.model.HttpBean;
import com.bbva.gateway.service.GatewayService;
import com.bbva.gateway.service.base.GatewayBaseService;
import com.bbva.gateway.service.impl.HttpGatewayServiceImpl;
import com.bbva.gateway.service.records.PersonalData;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
import org.apache.kafka.common.record.TimestampType;
import org.codehaus.jackson.map.ObjectMapper;
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

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PowerMockIgnore("javax.net.ssl.*")
@PrepareForTest({RepositoryImpl.class, Event.class, HandlerContextImpl.class, GatewayBaseService.class, RetrofitClient.class, Response.class})
public class HttpGatewayServiceTest {

    @DisplayName("Create service ok")
    @Test
    public void startRestOk() {
        final GatewayService service = new HttpGatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);

        service.init(ConfigBuilder.create(configAnnotation), "baseName");
        service.postInitActions();

        Assertions.assertAll("HttpGatewayService",
                () -> Assertions.assertNotNull(service)
        );
    }

    @DisplayName("Service call ok")
    @Test
    public void callOk() {
        PowerMockito.mockStatic(RetrofitClient.class);
        final Response response = PowerMockito.mock(Response.class);
        PowerMockito.when(RetrofitClient.call(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(response);

        final GatewayService service = new HttpGatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(ConfigBuilder.create(configAnnotation), "baseName");

        final Response callResult = ((HttpGatewayServiceImpl) service).call(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), new RecordHeaders()));

        Assertions.assertAll("HttpGatewayService",
                () -> Assertions.assertNotNull(service),
                () -> Assertions.assertEquals(response, callResult)
        );
    }

    @DisplayName("Process record ok")
    @Test
    public void processRecordOk() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.mockStatic(RetrofitClient.class);
        PowerMockito.whenNew(RepositoryImpl.class).withAnyArguments().thenReturn(PowerMockito.mock(RepositoryImpl.class));

        PowerMockito.whenNew(Event.class).withAnyArguments().thenReturn(PowerMockito.mock(Event.class));

        final Response response = PowerMockito.mock(Response.class);
        PowerMockito.when(response, "headers").thenReturn(new Headers.Builder().build());
        PowerMockito.when(RetrofitClient.call(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(response);

        final HttpGatewayServiceImpl service = new HttpGatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(ConfigBuilder.create(configAnnotation), "baseName");

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(false));
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("type"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("uuid"));

        service.processRecord(new HandlerContextImpl(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), recordHeaders), null, false));

        Assertions.assertAll("HttpGatewayService",
                () -> Assertions.assertNotNull(service)
        );
    }


    @DisplayName("Parse changelog from string ok")
    @Test
    public void parseChangelogFromStringOk() throws IOException {
        final HttpGatewayServiceImpl service = new HttpGatewayServiceImpl();

        final ObjectMapper mapper = new ObjectMapper();
        final HttpBean httpBean = new HttpBean(200, "body", new HashMap<>());
        final Response resp = service.parseChangelogFromString(mapper.writeValueAsString(httpBean));

        Assertions.assertAll("HttpGatewayService",
                () -> Assertions.assertNotNull(service),
                () -> Assertions.assertNotNull(resp),
                () -> Assertions.assertTrue(resp.isSuccessful())
        );
    }

    @DisplayName("Parse changelog from string ok")
    @Test
    public void parseChangelogFromStringException() {
        final HttpGatewayServiceImpl service = new HttpGatewayServiceImpl();

        final Response resp = service.parseChangelogFromString("tests");

        Assertions.assertAll("HttpGatewayService",
                () -> Assertions.assertNotNull(service),
                () -> Assertions.assertNull(resp)
        );
    }

    @DisplayName("Parse changelog to string ok")
    @Test
    public void parseChangelogToStringException() throws Exception {
        final HttpGatewayServiceImpl service = new HttpGatewayServiceImpl();

        final Response response = PowerMockito.mock(Response.class);
        PowerMockito.when(response, "headers").thenReturn(new Headers.Builder().build());
        PowerMockito.when(response, "body").thenReturn(ResponseBody.create(MediaType.get("application/json"), "{}"));
        final String resp = service.parseChangelogToString(response);

        Assertions.assertAll("HttpGatewayService",
                () -> Assertions.assertNotNull(resp)
        );
    }

    @DisplayName("Test HttpBean class")
    @Test
    public void testHttpBean() {
        final HttpBean bean = new HttpBean(200, "body", new HashMap<>());

        Assertions.assertAll("HttpBean",
                () -> Assertions.assertNotNull(bean.toString()),
                () -> Assertions.assertTrue(bean.hashCode() > 0),
                () -> Assertions.assertTrue(bean.equals(new HttpBean(200, "body", new HashMap<>()))),
                () -> Assertions.assertFalse(bean.equals(new HttpBean(400, "body2", new HashMap<>()))),
                () -> Assertions.assertFalse(bean.equals(null))
        );
    }
}