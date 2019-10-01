package com.bbva.dataprocessors.builders.dataflows;

import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;

/**
 * Dataflow builders interface
 */
public interface DataflowBuilder {

    /**
     * Initialize the builder
     *
     * @param context builder context
     */
    void init(DataflowProcessorContext context);

    /**
     * Build
     */
    void build();
}
