package com.bbva.ddd.application;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.ddd.common.CommonHelper;

public class HelperApplication extends CommonHelper {
    private static HelperApplication instance;

    public HelperApplication(final ApplicationConfig applicationConfig) {
        super(applicationConfig);
        instance = this;
    }

    public static HelperApplication get() {
        return instance;
    }


}
