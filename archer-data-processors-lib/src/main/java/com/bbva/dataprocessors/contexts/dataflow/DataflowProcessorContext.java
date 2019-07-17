package com.bbva.dataprocessors.contexts.dataflow;

import com.bbva.dataprocessors.contexts.ProcessorContext;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Map;

public interface DataflowProcessorContext extends ProcessorContext {

    Map<String, String> serdeProperties();

    StreamsBuilder streamsBuilder();

}
