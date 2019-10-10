package com.bbva.ddd.domain;

import com.bbva.common.config.AppConfig;
import com.bbva.ddd.common.CommonHelper;

/**
 * Helper to manage configurations and send events/commands from domain layer
 */
public final class HelperDomain extends CommonHelper {
    private static HelperDomain instance;
    private boolean replayMode;

    /**
     * Constructor
     *
     * @param configs configuration
     */
    private HelperDomain(final AppConfig configs) {
        super(configs);
    }

    /**
     * Create a helper instance
     *
     * @param configs configuration
     * @return instance
     */
    public static HelperDomain create(final AppConfig configs) {
        instance = new HelperDomain(configs);
        return instance;
    }

    /**
     * Check if helper is instanciated
     *
     * @return true/false
     */
    public static boolean isInstantiated() {
        return instance != null;
    }

    /**
     * Get actual instance of helper
     *
     * @return instance
     */
    public static HelperDomain get() {
        return instance;
    }

    /**
     * Check if helper is in replay mode
     *
     * @return true/false
     */
    public boolean isReplayMode() {
        return replayMode;
    }

    /**
     * Enable or disable the replay flag
     *
     * @param replayMode
     */
    public void setReplayMode(final boolean replayMode) {
        this.replayMode = replayMode;
    }

}
