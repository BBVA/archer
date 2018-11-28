package com.bbva.dataprocessors.builders.dataflows;

import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;

public interface DataflowBuilder {

    void init(DataflowProcessorContext context);

    void build();
}
