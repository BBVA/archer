package com.bbva.dataprocessors.contexts.dataflow;

import com.bbva.dataprocessors.contexts.ProcessorContext;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Map;

/**
 * Dataflow processor context interface
 */
public interface DataflowProcessorContext extends ProcessorContext {

    /**
     * Get serde properties
     *
     * @return properties
     */
    Map<String, String> serdeProperties();

    /**
     * Get stream builder
     *
     * @return builder
     */
    StreamsBuilder streamsBuilder();

}
