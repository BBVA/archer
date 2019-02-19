package com.bbva.ddd.domain;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.consumers.DefaultConsumer;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public abstract class RunnableConsumer<V extends SpecificRecordBase, T extends CRecord> extends DefaultConsumer<V, T>
        implements Runnable {

    private final String[] replayTopics;

    public RunnableConsumer(final int id, final List<String> topics, final Consumer<T> callback, final ApplicationConfig applicationConfig) {
        super(id, topics, callback, applicationConfig);
        this.replayTopics = (applicationConfig.get(ApplicationConfig.REPLAY_TOPICS) != null)
                ? applicationConfig.get(ApplicationConfig.REPLAY_TOPICS).toString().split(",") : new String[]{};
    }

    @Override
    public void run() {
        if (this.replayTopics.length > 0) {
            final HelperDomain helperDomain = HelperDomain.get();

            helperDomain.setReplayMode(true);
            replay(Arrays.asList(this.replayTopics));
            helperDomain.setReplayMode(false);
        }

        this.play();
    }

    public void shutdown() {
        this.closed.set(true);
        this.consumer.wakeup();
    }
}
