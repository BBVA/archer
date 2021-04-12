package com.bbva.gateway.service.base;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.dataprocessors.states.ReadableStore;
import com.bbva.dataprocessors.states.States;
import com.bbva.ddd.domain.changelogs.repository.RepositoryImpl;
import com.bbva.ddd.domain.handlers.contexts.HandlerContextImpl;
import com.bbva.gateway.GatewayTest;
import com.bbva.gateway.config.ConfigBuilder;
import com.bbva.gateway.config.annotations.Config;
import com.bbva.gateway.service.AsyncGatewayService;
import com.bbva.gateway.service.GatewayService;
import com.bbva.gateway.service.impl.*;
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
@PrepareForTest({GatewayBaseService.class, HandlerContextImpl.class, RepositoryImpl.class, RepositoryImpl.class, States.class, ReadableStore.class})
public class AsyncGatewayBaseServiceTest {

    @DisplayName("Create service ok")
    @Test
    public void startRestOk() {
        final AsyncGatewayService service = new AsyncGatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);

        service.init(ConfigBuilder.create(configAnnotation), "baseName");
        service.postInitActions();

        Assertions.assertAll("AsyncGatewayBaseService",
                () -> Assertions.assertNotNull(service)
        );
    }

    @DisplayName("Service call ok")
    @Test
    public void callOk() {
        final GatewayService service = new AsyncGatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(ConfigBuilder.create(configAnnotation), "baseName");

        final Person callResult = ((AsyncGatewayServiceImpl) service).call(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                null, new RecordHeaders()));

        Assertions.assertAll("AsyncGatewayBaseService",
                () -> Assertions.assertNotNull(service),
                () -> Assertions.assertEquals("name", callResult.getName())
        );
    }

    @DisplayName("Process record ok")
    @Test
    public void processRecordOk() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.whenNew(RepositoryImpl.class).withAnyArguments().thenReturn(PowerMockito.mock(RepositoryImpl.class));

        final AsyncGatewayServiceImpl service = new AsyncGatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(ConfigBuilder.create(configAnnotation), "baseName");

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(false));

        service.processRecord(new HandlerContextImpl(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), recordHeaders), null, false));

        Assertions.assertAll("GatewayBaseService",
                () -> Assertions.assertNotNull(service)
        );
    }

    @DisplayName("Process reply record ok")
    @Test
    public void processReplyRecordOk() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.whenNew(RepositoryImpl.class).withAnyArguments().thenReturn(PowerMockito.mock(RepositoryImpl.class));
        PowerMockito.mockStatic(States.class);

        final ObjectMapper om = new ObjectMapper();

        final ReadableStore store = PowerMockito.mock(ReadableStore.class);
        final States states = PowerMockito.mock(States.class);
        PowerMockito.when(States.get()).thenReturn(states);
        PowerMockito.when(states.getStore(Mockito.any())).thenReturn(store);

        final TransactionChangelog transactionChangelog = new TransactionChangelog();
        transactionChangelog.setOutput(om.writeValueAsString(new Person("name")));
        PowerMockito.when(store, "findById", Mockito.any()).thenReturn(transactionChangelog);

        final AsyncGatewayServiceImpl service = new AsyncGatewayServiceImpl();
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        service.init(ConfigBuilder.create(configAnnotation), "baseName");

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(true));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("referenceKey"));

        service.processRecord(new HandlerContextImpl(new CRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), recordHeaders), null, false));

        Assertions.assertNotNull(service);
    }

    @DisplayName("Save changelog ok")
    @Test
    public void saveChangelogOk() {

        final AsyncGatewayServiceImpl service = new AsyncGatewayServiceImpl();
        AsyncGatewayBaseService.saveChangelog("iden", "body");

        Assertions.assertAll("GatewayBaseService",
                () -> Assertions.assertNotNull(service)
        );
    }

    @DisplayName("Save null changelog ok")
    @Test
    public void saveNullChangelogOk() {

        final AsyncGatewayServiceImpl service = new AsyncGatewayServiceImpl();
        AsyncGatewayBaseService.saveChangelog("iden", "body");

        Assertions.assertAll("GatewayBaseService",
                () -> Assertions.assertNotNull(service)
        );
    }

}

