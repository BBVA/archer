package com.bbva.ddd.application;

import com.bbva.common.config.AppConfig;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.ddd.domain.commands.producers.Command;
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
@PrepareForTest(HelperApplication.class)
public class HelperApplicationTest {

    @DisplayName("Create application helper ok")
    @Test
    public void createHelperApplicationOk() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        final AppConfig appConfig = new AppConfig();
        final HelperApplication helper = HelperApplication.create(appConfig);

        Assertions.assertAll("HelperApplication",
                () -> Assertions.assertNotNull(helper),
                () -> Assertions.assertEquals(helper, HelperApplication.get())
        );
    }


    @DisplayName("Create command ok")
    @Test
    public void createCommandOk() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        final AppConfig appConfig = new AppConfig();
        final HelperApplication helper = HelperApplication.create(appConfig);

        Assertions.assertAll("HelperApplication",
                () -> Assertions.assertNotNull(helper),
                () -> Assertions.assertNotNull(helper.command(Command.Action.CREATE_ACTION)),
                () -> Assertions.assertNotNull(helper.command("another_action"))
        );
    }

    @DisplayName("Create event ok")
    @Test
    public void createEventOk() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        final AppConfig appConfig = new AppConfig();
        final HelperApplication helper = HelperApplication.create(appConfig);

        Assertions.assertAll("HelperApplication",
                () -> Assertions.assertNotNull(helper),
                () -> Assertions.assertNotNull(helper.event("event_name"))
        );
    }

}
