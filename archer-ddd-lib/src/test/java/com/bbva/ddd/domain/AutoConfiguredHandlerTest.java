package com.bbva.ddd.domain;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.ddd.domain.annotations.Changelog;
import com.bbva.ddd.domain.annotations.Command;
import com.bbva.ddd.domain.annotations.Event;
import com.bbva.ddd.domain.annotations.Handler;
import com.bbva.ddd.domain.callback.ActionHandler;
import com.bbva.ddd.domain.changelogs.consumers.ChangelogHandlerContext;
import com.bbva.ddd.domain.changelogs.repository.RepositoryImpl;
import com.bbva.ddd.domain.commands.consumers.CommandHandlerContext;
import com.bbva.ddd.domain.events.consumers.EventHandlerContext;
import com.bbva.ddd.util.AnnotationUtil;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({AnnotationUtil.class, RepositoryImpl.class})
@Handler
public class AutoConfiguredHandlerTest {

    @DisplayName("Create auto configured handler ok")
    @Test
    public void createhandler() {
        final AutoConfiguredHandler handler = new AutoConfiguredHandler();

        Assertions.assertAll("AutoConfiguredHandler",
                () -> Assertions.assertNotNull(handler)
        );
    }

    @DisplayName("Check annotated methods ok")
    @Test
    public void checkAnnotatedActions() throws Exception {
        final List<Class> lstClasses = new ArrayList<>();
        lstClasses.add(ActionHandler.class);

        PowerMockito.spy(AnnotationUtil.class);
        final Method m = Whitebox.getMethod(AnnotationUtil.class, "findAllAnnotatedClasses", Class.class);
        PowerMockito.doReturn(lstClasses).when(AnnotationUtil.class, m).withArguments(com.bbva.ddd.domain.annotations.Handler.class);

        final AutoConfiguredHandler handler = new AutoConfiguredHandler();

        Assertions.assertAll("AutoConfiguredHandler",
                () -> Assertions.assertNotNull(handler),
                () -> Assertions.assertEquals(1, handler.commandsSubscribed().size()),
                () -> Assertions.assertEquals(1, handler.dataChangelogsSubscribed().size()),
                () -> Assertions.assertEquals(1, handler.eventsSubscribed().size())
        );
    }

    @DisplayName("Create auto configured handler and get subscriptions ok")
    @Test
    public void getSubscriptions() {
        final AutoConfiguredHandler handler = new AutoConfiguredHandler();

        Assertions.assertAll("AutoConfiguredHandler",
                () -> Assertions.assertNotNull(handler),
                () -> Assertions.assertEquals(1, handler.commandsSubscribed().size()),
                () -> Assertions.assertEquals(1, handler.dataChangelogsSubscribed().size()),
                () -> Assertions.assertEquals(1, handler.eventsSubscribed().size())
        );
    }

    @DisplayName("process not handle command ok")
    @Test
    @Command(commandAction = "action", baseName = "base")
    public void processCommand() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        ConfigBuilder.create();

        final List<Class> lstClasses = new ArrayList<>();
        lstClasses.add(ActionHandler.class);

        PowerMockito.spy(AnnotationUtil.class);
        final Method m = Whitebox.getMethod(AnnotationUtil.class, "findAllAnnotatedClasses", Class.class);
        PowerMockito.doReturn(lstClasses).when(AnnotationUtil.class, m).withArguments(com.bbva.ddd.domain.annotations.Handler.class);

        final AutoConfiguredHandler handler = new AutoConfiguredHandler();

        Exception ex = null;
        try {
            final RecordHeaders recordHeaders = new RecordHeaders();
            recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue(CommandHeaderType.TYPE_VALUE));
            recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("create"));
            handler.processCommand(new CommandHandlerContext(new CRecord("commandName" + AppConfig.COMMANDS_RECORD_NAME_SUFFIX, 1, 1, new Date().getTime(),
                    TimestampType.CREATE_TIME, "key", null, recordHeaders)));
        } catch (final Exception e) {
            ex = e;
        }
        Assertions.assertNull(ex);
    }

    @DisplayName("process not handle event ok")
    @Test
    @Event(baseName = "base")
    public void processEvent() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        ConfigBuilder.create();

        final AutoConfiguredHandler handler = new AutoConfiguredHandler();
        Exception ex = null;
        try {
            final RecordHeaders recordHeaders = new RecordHeaders();
            recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue(CommandHeaderType.TYPE_VALUE));
            recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("action"));
            handler.processEvent(new EventHandlerContext(new CRecord("topic" + AppConfig.COMMANDS_RECORD_NAME_SUFFIX, 1, 1, new Date().getTime(),
                    TimestampType.CREATE_TIME, "key", null, recordHeaders)));
        } catch (final Exception e) {
            ex = e;
        }
        Assertions.assertNull(ex);
    }

    @DisplayName("process not handle data changelog ok")
    @Test
    @Changelog(baseName = "base")
    public void processDataChangelog() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        ConfigBuilder.create();
        final AutoConfiguredHandler handler = new AutoConfiguredHandler();
        Exception ex = null;
        try {
            final RecordHeaders recordHeaders = new RecordHeaders();
            recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue(CommandHeaderType.TYPE_VALUE));
            recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("action"));
            handler.processDataChangelog(new ChangelogHandlerContext(new CRecord("topic" + AppConfig.COMMANDS_RECORD_NAME_SUFFIX, 1, 1, new Date().getTime(),
                    TimestampType.CREATE_TIME, "key", null, recordHeaders)));
        } catch (final Exception e) {
            ex = e;
        }
        Assertions.assertNull(ex);
    }

}
