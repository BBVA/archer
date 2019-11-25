package com.bbva.common.managers;

import com.bbva.common.config.AppConfig;

import java.util.Arrays;

public class RunnableManager implements Runnable {

    private final String[] replayTopics;
    private final Manager manager;

    /**
     * Constructor
     *
     * @param manager   consumer Adapter
     * @param appConfig configuration
     */
    public RunnableManager(final Manager manager, final AppConfig appConfig) {
        replayTopics = (appConfig.get(AppConfig.REPLAY_TOPICS) != null)
                ? appConfig.get(AppConfig.REPLAY_TOPICS).toString().split(",") : new String[]{};

        this.manager = manager;
    }

    /**
     * Start the consumption, first replaying the specify topics
     */
    @Override
    public void run() {
        if (replayTopics.length > 0) {
            manager.replay(Arrays.asList(replayTopics));
        }

        manager.play();
    }

    /**
     * Close the consumer
     */
    public void shutdown() {
        manager.shutdown();
    }
}
