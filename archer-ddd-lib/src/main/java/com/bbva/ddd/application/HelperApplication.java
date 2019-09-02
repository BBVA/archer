package com.bbva.ddd.application;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.ddd.common.CommonHelper;

public final class HelperApplication extends CommonHelper {
    private static HelperApplication instance;

    private HelperApplication(final ApplicationConfig applicationConfig) {
        super(applicationConfig);
    }

    public static HelperApplication create(final ApplicationConfig configs) {
        instance = new HelperApplication(configs);
        return instance;
    }

    public static HelperApplication get() {
        return instance;
    }

    public static boolean isInstantiated() {
        return instance != null;
    }
}
