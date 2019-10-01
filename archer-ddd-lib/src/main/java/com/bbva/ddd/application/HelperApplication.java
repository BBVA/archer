package com.bbva.ddd.application;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.ddd.common.CommonHelper;

/**
 * Helper to manage configurations and send events/commands from application layer
 */
public final class HelperApplication extends CommonHelper {

    private static HelperApplication instance;

    /**
     * Constructor
     *
     * @param applicationConfig general configuration
     */
    private HelperApplication(final ApplicationConfig applicationConfig) {
        super(applicationConfig);
    }

    /**
     * Create a helper instance
     *
     * @param configs configuration
     * @return instance
     */
    public static HelperApplication create(final ApplicationConfig configs) {
        instance = new HelperApplication(configs);
        return instance;
    }

    /**
     * Get actual instance of the helper
     *
     * @return instance
     */
    public static HelperApplication get() {
        return instance;
    }

    /**
     * Check if helper is created
     *
     * @return true/false
     */
    public static boolean isInstantiated() {
        return instance != null;
    }
}
