package com.bbva.dataprocessors.builders.dataflows;

import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

/**
 * Dataflow processor builder
 */
public class DataflowProcessorBuilder implements ProcessorBuilder {

    private static final Logger logger = LoggerFactory.getLogger(DataflowProcessorBuilder.class);

    private final DataflowBuilder stateBuilder;
    private DataflowProcessorContext context;
    private KafkaStreams streams;

    /**
     * Constructor
     *
     * @param stateBuilder builder
     */
    public DataflowProcessorBuilder(final DataflowBuilder stateBuilder) {
        this.stateBuilder = stateBuilder;
    }

    /**
     * Initialize the builder
     *
     * @param context builder context
     */
    public void init(final DataflowProcessorContext context) {
        this.context = context;
        stateBuilder.init(context);
    }

    /**
     * Build the builder
     */
    @Override
    public void build() {
        stateBuilder.build();
    }

    /**
     * Start the processor
     */
    @Override
    public void start() {
        final Topology topology = context.streamsBuilder().build();
        streams = new KafkaStreams(topology, context.configs().streams().get());

        final TopologyDescription topologyDescription = topology.describe();
        logger.info("******************************** Topology Description for: {}", context.name());
        logger.info("GlobalStores: {}", topologyDescription.globalStores());
        logger.info("Subtopologies: {}", topologyDescription.subtopologies());
        logger.info("********************************");

        streams.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
            // here you should examine the exception and perform an appropriate action!
            logger.error("Error starting DataflowProcessorBuilder", e);
        });

        streams.start();
    }

    /**
     * Return the streams
     *
     * @return streams
     */
    @Override
    public KafkaStreams streams() {
        return streams;
    }

    /**
     * Close the streams
     */
    @Override
    public void close() {
        streams.close();
    }

    /**
     * get context name
     *
     * @return the name
     */
    public String getName() {
        return context.name();
    }
}
