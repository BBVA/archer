package com.bbva.ddd.domain;

import com.bbva.common.config.ApplicationConfig;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.Assumptions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

@RunWith(JUnit5.class)
public class HelperDomainTest {

    @DisplayName("Create helper ok")
    @Test
    public void createHelperDomainOk() {
        Assumptions.assumeFalse(HelperDomain.isInstantiated());

        final ApplicationConfig appConfig = new ApplicationConfig();
        final HelperDomain helper = HelperDomain.create(appConfig);
        helper.setReplayMode(true);

        Assertions.assertAll("HelperDomain",
                () -> Assertions.assertNotNull(helper),
                () -> Assertions.assertEquals(appConfig, helper.getConfig()),
                () -> Assertions.assertEquals(helper, HelperDomain.get()),
                () -> Assertions.assertTrue(HelperDomain.isInstantiated()),
                () -> Assertions.assertTrue(helper.isReplayMode())
        );
    }

}
