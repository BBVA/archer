package com.bbva.gateway.service;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.dataprocessors.states.ReadableStore;
import com.bbva.ddd.domain.changelogs.repository.RepositoryImpl;
import com.bbva.ddd.domain.events.producers.Event;
import com.bbva.ddd.domain.handlers.contexts.HandlerContextImpl;
import com.bbva.ddd.util.StoreUtil;
import com.bbva.gateway.GatewayTest;
import com.bbva.gateway.config.ConfigBuilder;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.config.annotations.Config;
import com.bbva.gateway.service.impl.GatewayService;
import com.bbva.gateway.service.impl.GatewayServiceImpl;
import com.bbva.gateway.service.impl.beans.Person;
import com.bbva.gateway.service.records.PersonalData;
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
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({RepositoryImpl.class, Event.class, HandlerContextImpl.class, GatewayServiceImpl.class, GatewayService.class, StoreUtil.class, ReadableStore.class})
public class GatewayServiceTest {

    @DisplayName("Create service ok")
    @Test
    public void startRestOk() {
        final IGatewayService service = new GatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);

        service.init(ConfigBuilder.create(configAnnotation), "baseName");
        service.postInitActions();

        Assertions.assertAll("GatewayService",
                () -> Assertions.assertNotNull(service)
        );
    }

    @DisplayName("Service call ok")
    @Test
    public void callOk() {
        final IGatewayService service = new GatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);

        final GatewayConfig configuration = ConfigBuilder.create(configAnnotation);
        configuration.gateway().remove(GatewayConfig.GatewayProperties.GATEWAY_RETRY);
        service.init(configuration, "baseName");

        final Person callResult = ((GatewayServiceImpl) service).call(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                null, new RecordHeaders()));

        Assertions.assertAll("GatewayService",
                () -> Assertions.assertNotNull(service),
                () -> Assertions.assertEquals("result", callResult.getName())
        );
    }

    @DisplayName("Service call ok")
    @Test
    public void callWithoutRetryOk() {
        final IGatewayService service = new GatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);

        final GatewayConfig configuration = ConfigBuilder.create(configAnnotation);
        final Map retryPolicy = new LinkedHashMap();
        retryPolicy.put(GatewayConfig.GatewayProperties.GATEWAY_RETRY_ENABLED, false);
        configuration.gateway().put(GatewayConfig.GatewayProperties.GATEWAY_RETRY, retryPolicy);
        service.init(configuration, "baseName");

        final Person callResult = ((GatewayServiceImpl) service).call(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                null, new RecordHeaders()));

        Assertions.assertAll("GatewayService",
                () -> Assertions.assertNotNull(service),
                () -> Assertions.assertEquals("result", callResult.getName())
        );
    }

    @DisplayName("Process record ok")
    @Test
    public void processRecordOk() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.whenNew(RepositoryImpl.class).withAnyArguments().thenReturn(PowerMockito.mock(RepositoryImpl.class));

        PowerMockito.whenNew(Event.class).withAnyArguments().thenReturn(PowerMockito.mock(Event.class));

        final GatewayServiceImpl service = new GatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(ConfigBuilder.create(configAnnotation), "baseName");

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(false));
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("type"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("uuid"));

        service.processRecord(new HandlerContextImpl(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), recordHeaders), null, false));

        Assertions.assertAll("GatewayService",
                () -> Assertions.assertNotNull(service)
        );
    }

    @DisplayName("Process reply record ok")
    @Test
    public void processReplyOk() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.mockStatic(StoreUtil.class);

        PowerMockito.whenNew(Event.class).withAnyArguments().thenReturn(PowerMockito.mock(Event.class));

        final ReadableStore store = PowerMockito.mock(ReadableStore.class);

        PowerMockito.when(StoreUtil.getStore(Mockito.any())).thenReturn(store);

        final ObjectMapper mapper = new ObjectMapper();
        final TransactionChangelog transactionChangelog = new TransactionChangelog();
        transactionChangelog.setOutput(mapper.writeValueAsString(new Person("name")));
        PowerMockito.when(store, "findById", Mockito.any()).thenReturn(transactionChangelog);

        final GatewayServiceImpl service = PowerMockito.spy(new GatewayServiceImpl());
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(ConfigBuilder.create(configAnnotation), "baseName");
        PowerMockito.doReturn(new Person("name")).when(service, "parseChangelogFromString", Mockito.anyString());

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(true));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("referenceKey"));
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("type"));

        service.processRecord(new HandlerContextImpl(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), recordHeaders), null, false));

        Assertions.assertNotNull(service);
    }


    @DisplayName("Process reply record without changelog ok")
    @Test
    public void processReplyWithoutChangelogOk() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.mockStatic(StoreUtil.class);

        final ReadableStore store = PowerMockito.mock(ReadableStore.class);

        PowerMockito.when(StoreUtil.getStore(Mockito.any())).thenReturn(store);

        final ObjectMapper mapper = new ObjectMapper();
        final TransactionChangelog transactionChangelog = new TransactionChangelog();
        transactionChangelog.setOutput(mapper.writeValueAsString(new Person("name")));
        PowerMockito.when(store, "findById", Mockito.any()).thenReturn(null);

        final GatewayServiceImpl service = PowerMockito.spy(new GatewayServiceImpl());
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(ConfigBuilder.create(configAnnotation), "baseName");
        PowerMockito.doReturn(new Person("name")).when(service, "parseChangelogFromString", Mockito.anyString());

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(true));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("referenceKey"));

        service.processRecord(new HandlerContextImpl(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), recordHeaders), null, false));

        Assertions.assertNotNull(service);
    }
}
