package com.bbva.ddd.application;

import com.bbva.common.config.ApplicationConfig;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.Assumptions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

@RunWith(JUnit5.class)
public class HelperApplicationTest {

    @DisplayName("Create common helper ok")
    @Test
    public void createHelperApplicationOk() {
        Assumptions.assumeFalse(HelperApplication.isInstantiated());

        final ApplicationConfig appConfig = new ApplicationConfig();
        final HelperApplication helper = HelperApplication.create(appConfig);

        Assertions.assertAll("HelperApplication",
                () -> Assertions.assertNotNull(helper),
                () -> Assertions.assertEquals(appConfig, helper.getConfig()),
                () -> Assertions.assertEquals(helper, HelperApplication.get()),
                () -> Assertions.assertTrue(HelperApplication.isInstantiated())
        );
    }

}
