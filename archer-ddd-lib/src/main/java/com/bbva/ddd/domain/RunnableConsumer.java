package com.bbva.ddd.domain;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.consumers.DefaultConsumer;
import org.apache.avro.specific.SpecificRecordBase;

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
public abstract class RunnableConsumer<V extends SpecificRecordBase, T extends CRecord> extends DefaultConsumer<V, T>
        implements Runnable {

    private final String[] replayTopics;

    /**
     * Constructor
     *
     * @param id                consumer id
     * @param topics            list of topics to subscribe
     * @param callback          callback to manage new events
     * @param applicationConfig configuration
     */
    public RunnableConsumer(final int id, final List<String> topics, final Consumer<T> callback, final ApplicationConfig applicationConfig) {
        super(id, topics, callback, applicationConfig);
        replayTopics = (applicationConfig.get(ApplicationConfig.REPLAY_TOPICS) != null)
                ? applicationConfig.get(ApplicationConfig.REPLAY_TOPICS).toString().split(",") : new String[]{};
    }

    /**
     * Start the consumer
     */
    @Override
    public void run() {
        if (replayTopics.length > 0) {
            final HelperDomain helperDomain = HelperDomain.get();

            helperDomain.setReplayMode(true);
            replay(Arrays.asList(replayTopics));
            helperDomain.setReplayMode(false);
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
