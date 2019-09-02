package com.bbva.ddd.domain;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.ddd.common.CommonHelper;

public final class HelperDomain extends CommonHelper {
    private static HelperDomain instance;
    private boolean replayMode;

    private HelperDomain(final ApplicationConfig configs) {
        super(configs);
    }

    public static HelperDomain create(final ApplicationConfig configs) {
        instance = new HelperDomain(configs);
        return instance;
    }

    public static boolean isInstantiated() {
        return instance != null;
    }

    public static HelperDomain get() {
        return instance;
    }

    public boolean isReplayMode() {
        return replayMode;
    }

    public void setReplayMode(final boolean replayMode) {
        this.replayMode = replayMode;
    }

}
