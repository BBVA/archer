package com.bbva.common.managers;

import java.util.List;

public interface Manager {

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
