package com.bbva.common.consumers.adapters;

import java.util.List;

public interface ConsumerAdapter {

    /**
     * Replay a list of topics
     *
     * @param topics list of topics
     */
    void replay(final List<String> topics);

    /**
     * Start to consume records
     */
    void play();

    /**
     * Stop de consumption and the process
     */
    void shutdown();
}
