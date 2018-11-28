package com.bbva.ddd.domain;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.consumers.DefaultConsumer;
import com.bbva.ddd.ApplicationServices;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public abstract class RunnableConsumer<V extends SpecificRecordBase, T extends CRecord> extends DefaultConsumer<V, T>
        implements Runnable {

    private final String[] replayTopics;

    public RunnableConsumer(int id, List<String> topics, Consumer<T> callback, ApplicationConfig applicationConfig) {
        super(id, topics, callback, applicationConfig);
        this.replayTopics = (applicationConfig.get(ApplicationConfig.REPLAY_TOPICS) != null)
                ? applicationConfig.get(ApplicationConfig.REPLAY_TOPICS).toString().split(",") : new String[] {};
    }

    @Override
    public void run() {
        if (this.replayTopics.length > 0) {
            ApplicationServices app = ApplicationServices.get();
            app.setReplayMode(true);
            this.replay(Arrays.asList(this.replayTopics));
            app.setReplayMode(false);
            this.play();
        } else {
            this.play();
        }

    }

    public void shutdown() {
        this.closed.set(true);
        this.consumer.wakeup();
    }
}
