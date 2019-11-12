package com.bbva.common.consumers.adapters;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.callback.ConsumerCallback;

import java.util.Arrays;
import java.util.List;

public class RunnableConsumerAdapter implements Runnable {

    private final String[] replayTopics;
    private final ConsumerAdapter consumerAdapter;

    /**
     * Constructor
     *
     * @param eventStore event store type
     * @param id         of consumer
     * @param sources    sources to consume
     * @param callback   callback to manage the messages
     * @param appConfig  configuration
     */
    public RunnableConsumerAdapter(final ConsumerAdapterFactory.EventStores eventStore, final int id, final List<String> sources, final ConsumerCallback callback, final AppConfig appConfig) {
        replayTopics = (appConfig.get(AppConfig.REPLAY_TOPICS) != null)
                ? appConfig.get(AppConfig.REPLAY_TOPICS).toString().split(",") : new String[]{};

        consumerAdapter = ConsumerAdapterFactory.create(eventStore, id, sources, callback, appConfig);
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
