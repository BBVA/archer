package com.bbva.gateway.handlers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.ddd.domain.changelogs.repository.RepositoryImpl;
import com.bbva.ddd.domain.commands.consumers.CommandHandlerContext;
import com.bbva.ddd.domain.commands.consumers.CommandRecord;
import com.bbva.ddd.domain.events.consumers.EventHandlerContext;
import com.bbva.ddd.domain.events.consumers.EventRecord;
import com.bbva.gateway.GatewayTest;
import com.bbva.gateway.config.ConfigBuilder;
import com.bbva.gateway.config.annotations.Config;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.Date;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({RepositoryImpl.class})
public class GatewayHandlerTest {

    @DisplayName("Create auto configured handler ok")
    @Test
    public void createHandler() {

        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);

        final GatewayHandler handler = new GatewayHandler("com.bbva", ConfigBuilder.create(configAnnotation));

        Assertions.assertAll("AutoConfiguredHandler",
                () -> Assertions.assertNotNull(handler)
        );
    }

    @DisplayName("Create gateway configured handler and get subscriptions ok")
    @Test
    public void getSubscriptions() {
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        final GatewayHandler handler = new GatewayHandler("com.bbva", ConfigBuilder.create(configAnnotation));

        Assertions.assertAll("AutoConfiguredHandler",
                () -> Assertions.assertNotNull(handler),
                () -> Assertions.assertEquals(1, handler.commandsSubscribed().size()),
                () -> Assertions.assertEquals(1, handler.eventsSubscribed().size()),
                () -> Assertions.assertEquals(1, handler.getCommandServices().size()),
                () -> Assertions.assertEquals(1, handler.getEventServices().size())
        );
    }

    @DisplayName("process not handle command ok")
    @Test
    public void processCommand() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        final GatewayHandler handler = new GatewayHandler("com.bbva", ConfigBuilder.create(configAnnotation));

        Exception ex = null;
        try {
            final RecordHeaders recordHeaders = new RecordHeaders();
            recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue(CommandHeaderType.TYPE_VALUE));
            recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("create"));
            handler.processCommand(new CommandHandlerContext(new CommandRecord("commandName" + AppConfig.COMMANDS_RECORD_NAME_SUFFIX, 1, 1, new Date().getTime(),
                    TimestampType.CREATE_TIME, "key", null, recordHeaders), null, false));
        } catch (final Exception e) {
            ex = e;
        }
        Assertions.assertNull(ex);
    }

    @DisplayName("process not handle event ok")
    @Test
    public void processEvent() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        final GatewayHandler handler = new GatewayHandler("com.bbva", ConfigBuilder.create(configAnnotation));

        Exception ex = null;
        try {
            final RecordHeaders recordHeaders = new RecordHeaders();
            recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue(CommandHeaderType.TYPE_VALUE));
            recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("action"));
            handler.processEvent(new EventHandlerContext(new EventRecord("topic" + AppConfig.COMMANDS_RECORD_NAME_SUFFIX, 1, 1, new Date().getTime(),
                    TimestampType.CREATE_TIME, "key", null, recordHeaders), null, false));
        } catch (final Exception e) {
            ex = e;
        }
        Assertions.assertNull(ex);
    }


}
