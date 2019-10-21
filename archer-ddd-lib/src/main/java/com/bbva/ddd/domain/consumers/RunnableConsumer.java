package com.bbva.ddd.domain.consumers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.DefaultConsumer;
import com.bbva.common.consumers.contexts.ConsumerContext;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Default consumer implementation as runnable process
 * Example of consumer run in a new thread:
 * <pre>
 * {@code
 *  final RunnableConsumer consumer = new RunnableConsumer(0, Collections.singletonList( "topic_test" ), null, configuration) {
 *      @Override public CRecord message(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType, final String key, final SpecificRecordBase value, final RecordHeaders headers) {
 *          System.out.println(value);
 *          return null;
 *      }
 *  };
 *  final ExecutorService executor = Executors.newFixedThreadPool(1);
 *  executor.submit(consumer);
 * }</pre>
 */
public abstract class RunnableConsumer<T extends ConsumerContext> extends DefaultConsumer<T>
        implements Runnable {

    private final String[] replayTopics;

    /**
     * Constructor
     *
     * @param id        consumer id
     * @param topics    list of topics to subscribe
     * @param callback  callback to manage new events
     * @param appConfig configuration
     */
    public RunnableConsumer(final int id, final List<String> topics, final Consumer<T> callback, final AppConfig appConfig) {
        super(id, topics, callback, appConfig);
        replayTopics = (appConfig.get(AppConfig.REPLAY_TOPICS) != null)
                ? appConfig.get(AppConfig.REPLAY_TOPICS).toString().split(",") : new String[]{};
    }

    /**
     * Start the consumer
     */
    @Override
    public void run() {
        if (replayTopics.length > 0) {
            replay(Arrays.asList(replayTopics));
        }

        play();
    }

    /**
     * Close the consumer
     */
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
