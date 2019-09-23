package com.bbva.dataprocessors.builders.dataflows.states;

/**
 * Table state builders interface
 */
public interface TableStateBuilder extends StateDataflowBuilder {

    /**
     * Get the source base name
     *
     * @return source base name
     */
    String sourceTopicName();
}
