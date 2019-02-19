package com.bbva.ddd.domain;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.ddd.common.CommonHelper;

public class HelperDomain extends CommonHelper {
    private static HelperDomain instance;
    private boolean replayMode;

    public HelperDomain(final ApplicationConfig applicationConfig) {
        super(applicationConfig);
        instance = this;
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
