package com.bbva.gateway.service;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.dataprocessors.ReadableStore;
import com.bbva.ddd.domain.AggregateFactory;
import com.bbva.ddd.domain.HelperDomain;
import com.bbva.ddd.domain.events.write.Event;
import com.bbva.ddd.util.StoreUtil;
import com.bbva.gateway.GatewayTest;
import com.bbva.gateway.config.Configuration;
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

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({AggregateFactory.class, HelperDomain.class, GatewayServiceImpl.class, GatewayService.class, StoreUtil.class, ReadableStore.class})
public class GatewayServiceTest {

    @DisplayName("Create service ok")
    @Test
    public void startRestOk() {
        final IGatewayService service = new GatewayServiceImpl();
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
        final IGatewayService service = new GatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(new Configuration().init(configAnnotation), "baseName");

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
        PowerMockito.mockStatic(AggregateFactory.class);
        PowerMockito.mockStatic(HelperDomain.class);

        final HelperDomain helperDomain = PowerMockito.mock(HelperDomain.class);
        PowerMockito.when(HelperDomain.get()).thenReturn(helperDomain);
        PowerMockito.when(helperDomain, "sendEventTo", Mockito.anyString()).thenReturn(PowerMockito.mock(Event.class));

        final GatewayServiceImpl service = new GatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(new Configuration().init(configAnnotation), "baseName");

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(false));

        service.processRecord(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), recordHeaders));

        Assertions.assertAll("GatewayService",
                () -> Assertions.assertNotNull(service)
        );
    }


    @DisplayName("Process reply record ok")
    @Test
    public void processReplyOk() throws Exception {
        PowerMockito.mockStatic(AggregateFactory.class);
        PowerMockito.mockStatic(HelperDomain.class);
        PowerMockito.mockStatic(StoreUtil.class);

        final ReadableStore store = PowerMockito.mock(ReadableStore.class);

        final HelperDomain helperDomain = PowerMockito.mock(HelperDomain.class);
        PowerMockito.when(HelperDomain.get()).thenReturn(helperDomain);
        PowerMockito.when(helperDomain, "sendEventTo", Mockito.anyString()).thenReturn(PowerMockito.mock(Event.class));
        PowerMockito.when(StoreUtil.getStore(Mockito.any())).thenReturn(store);

        final ObjectMapper mapper = new ObjectMapper();
        final TransactionChangelog transactionChangelog = new TransactionChangelog();
        transactionChangelog.setOutput(mapper.writeValueAsString(new Person("name")));
        PowerMockito.when(store, "findById", Mockito.any()).thenReturn(transactionChangelog);

        final GatewayServiceImpl service = PowerMockito.spy(new GatewayServiceImpl());
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(new Configuration().init(configAnnotation), "baseName");
        PowerMockito.doReturn(new Person("name")).when(service, "parseChangelogFromString", Mockito.anyString());

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(true));
        recordHeaders.add(CommonHeaderType.REFERENCE_RECORD_KEY_KEY, new ByteArrayValue("referenceKey"));

        service.processRecord(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), recordHeaders));

        Assertions.assertNotNull(service);
    }

}
