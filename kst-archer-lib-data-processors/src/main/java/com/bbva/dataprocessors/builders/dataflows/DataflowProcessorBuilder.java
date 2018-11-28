package com.bbva.dataprocessors.builders.dataflows;

import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.log4j.Logger;

public class DataflowProcessorBuilder implements ProcessorBuilder {

    private final Logger logger;
    private final DataflowBuilder stateBuilder;
    private DataflowProcessorContext context;
    private KafkaStreams streams;

    public DataflowProcessorBuilder(DataflowBuilder stateBuilder) {
        this.stateBuilder = stateBuilder;
        logger = Logger.getLogger(DataflowProcessorBuilder.class);
    }

    public void init(DataflowProcessorContext context) {
        this.context = context;
        stateBuilder.init(context);
    }

    @Override
    public void build() {
        stateBuilder.build();
    }

    @Override
    public void start() {
        Topology topology = context.streamsBuilder().build();
        streams = new KafkaStreams(topology, context.configs().streams().get());

        TopologyDescription topologyDescription = topology.describe();
        logger.info("******************************** Topology Description for: " + context.name());
        logger.info("GlobalStores: " + topologyDescription.globalStores());
        logger.info("Subtopologies: " + topologyDescription.subtopologies());
        logger.info("********************************");

        streams.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
            // here you should examine the exception and perform an appropriate action!
            e.printStackTrace();
        });

        streams.start();
    }

    @Override
    public KafkaStreams streams() {
        return streams;
    }

    @Override
    public void close() {
        streams.close();
    }

    public String getName() {
        return context.name();
    }
}
