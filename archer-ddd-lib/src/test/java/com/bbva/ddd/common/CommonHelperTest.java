package com.bbva.ddd.common;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.TopicManager;
import com.bbva.ddd.domain.HelperDomain;
import com.bbva.ddd.domain.commands.write.Command;
import com.bbva.ddd.domain.events.write.Event;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({TopicManager.class, CommonHelper.class})
public class CommonHelperTest {

    @DisplayName("Create common helper ok")
    @Test
    public void createCommonHelperOk() {
        final AppConfig appConfig = new AppConfig();
        final CommonHelper helper = new CommonHelper(appConfig);

        Assertions.assertAll("commonHelper",
                () -> Assertions.assertNotNull(helper),
                () -> Assertions.assertEquals(appConfig, helper.getConfig())
        );
    }

    @DisplayName("send command ok")
    @Test
    public void sendCommandOk() throws Exception {
        PowerMockito.mockStatic(TopicManager.class);
        PowerMockito.whenNew(Command.class).withAnyArguments().thenReturn(PowerMockito.mock(Command.class));
        AppConfig appConfig = ConfigBuilder.create();

        final CommonHelper helper = new CommonHelper(appConfig);
        final HelperDomain helperDomain = HelperDomain.create(appConfig);
        final Command command = helper.sendCommandTo("command");
        final Command commandWrite = helper.persistsCommandTo("command");
        Assertions.assertAll("sendCommandTo",
                () -> Assertions.assertNotNull(command),
                () -> Assertions.assertEquals(command, commandWrite)
        );
    }


    @DisplayName("send event ok")
    @Test
    public void sendEventOk() throws Exception {
        PowerMockito.mockStatic(TopicManager.class);
        PowerMockito.whenNew(Event.class).withAnyArguments().thenReturn(PowerMockito.mock(Event.class));
        ConfigBuilder.create();
        final CommonHelper helper = new CommonHelper(new AppConfig());
        final Event event = helper.sendEventTo("event");
        final Event eventCached = helper.sendEventTo("event");

        Assertions.assertAll("sendEventTo",
                () -> Assertions.assertNotNull(event),
                () -> Assertions.assertEquals(event, eventCached)
        );
    }
}
