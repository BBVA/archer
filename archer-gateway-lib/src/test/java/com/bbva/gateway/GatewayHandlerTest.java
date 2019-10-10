package com.bbva.gateway;

import com.bbva.common.config.AppConfig;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.ddd.domain.commands.read.CommandHandlerContext;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.domain.events.read.EventHandlerContext;
import com.bbva.ddd.domain.events.read.EventRecord;
import com.bbva.gateway.config.Configuration;
import com.bbva.gateway.config.annotations.Config;
import com.bbva.gateway.util.CommandService;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RunWith(JUnit5.class)
public class GatewayHandlerTest {

    @DisplayName("Create auto configured handler ok")
    @Test
    public void createhandler() {

        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);

        final GatewayHandler handler = new GatewayHandler("com.bbva", new Configuration().init(configAnnotation));

        Assertions.assertAll("AutoConfiguredHandler",
                () -> Assertions.assertNotNull(handler)
        );
    }

    @DisplayName("Create gateway configured handler and get subscriptions ok")
    @Test
    public void getSubscriptions() {
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        final GatewayHandler handler = new GatewayHandler("com.bbva", new Configuration().init(configAnnotation));

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
        final List<Class> lstClasses = new ArrayList<>();
        lstClasses.add(CommandService.class);

        PowerMockito.spy(Configuration.class);
        final Method m = Whitebox.getMethod(Configuration.class, "getServiceClasses", String.class);
        PowerMockito.doReturn(lstClasses).when(Configuration.class, m).withArguments("com.bbva");

        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        final GatewayHandler handler = new GatewayHandler("com.bbva", new Configuration().init(configAnnotation));

        Exception ex = null;
        try {
            final RecordHeaders recordHeaders = new RecordHeaders();
            recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue(CommandHeaderType.TYPE_VALUE));
            recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("create"));
            handler.processCommand(new CommandHandlerContext(new CommandRecord("commandName" + AppConfig.COMMANDS_RECORD_NAME_SUFFIX, 1, 1, new Date().getTime(),
                    TimestampType.CREATE_TIME, "key", null, recordHeaders)));
        } catch (final Exception e) {
            ex = e;
        }
        Assertions.assertNull(ex);
    }

    @DisplayName("process not handle event ok")
    @Test
    public void processEvent() {
        final Config configAnnotation = GatewayTest.class.getAnnotation(Config.class);
        final GatewayHandler handler = new GatewayHandler("com.bbva", new Configuration().init(configAnnotation));

        Exception ex = null;
        try {
            final RecordHeaders recordHeaders = new RecordHeaders();
            recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue(CommandHeaderType.TYPE_VALUE));
            recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("action"));
            handler.processEvent(new EventHandlerContext(new EventRecord("topic" + AppConfig.COMMANDS_RECORD_NAME_SUFFIX, 1, 1, new Date().getTime(),
                    TimestampType.CREATE_TIME, "key", null, recordHeaders)));
        } catch (final Exception e) {
            ex = e;
        }
        Assertions.assertNull(ex);
    }


}
