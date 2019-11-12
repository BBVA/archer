package com.bbva.common.consumers.adapters;

import com.bbva.common.config.AppConfig;

import java.util.Arrays;

public class RunnableConsumerAdapter implements Runnable {

    private final String[] replayTopics;
    private final ConsumerAdapter consumerAdapter;

    /**
     * Constructor
     *
     * @param consumerAdapter consumer Adapter
     * @param appConfig       configuration
     */
    public RunnableConsumerAdapter(final ConsumerAdapter consumerAdapter, final AppConfig appConfig) {
        replayTopics = (appConfig.get(AppConfig.REPLAY_TOPICS) != null)
                ? appConfig.get(AppConfig.REPLAY_TOPICS).toString().split(",") : new String[]{};

        this.consumerAdapter = consumerAdapter;
    }

    /**
     * Start the consumption, first replaying the specify topics
     */
    @Override
    public void run() {
        if (replayTopics.length > 0) {
            consumerAdapter.replay(Arrays.asList(replayTopics));
        }

        consumerAdapter.play();
    }

    /**
     * Close the consumer
     */
    public void shutdown() {
        consumerAdapter.shutdown();
    }
}
