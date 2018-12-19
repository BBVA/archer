package com.bbva.dataprocessors.builders.dataflows;

import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

public class DataflowProcessorBuilder implements ProcessorBuilder {

    private static final LoggerGen logger = LoggerGenesis.getLogger(DataflowProcessorBuilder.class.getName());
    private final DataflowBuilder stateBuilder;
    private DataflowProcessorContext context;
    private KafkaStreams streams;

    public DataflowProcessorBuilder(final DataflowBuilder stateBuilder) {
        this.stateBuilder = stateBuilder;
    }

    public void init(final DataflowProcessorContext context) {
        this.context = context;
        stateBuilder.init(context);
    }

    @Override
    public void build() {
        stateBuilder.build();
    }

    @Override
    public void start() {
        final Topology topology = context.streamsBuilder().build();
        streams = new KafkaStreams(topology, context.configs().streams().get());

        final TopologyDescription topologyDescription = topology.describe();
        logger.info("******************************** Topology Description for: " + context.name());
        logger.info("GlobalStores: " + topologyDescription.globalStores());
        logger.info("Subtopologies: " + topologyDescription.subtopologies());
        logger.info("********************************");

        streams.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
            // here you should examine the exception and perform an appropriate action!
            logger.error("Error starting DataflowProcessorBuilder", e);
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
