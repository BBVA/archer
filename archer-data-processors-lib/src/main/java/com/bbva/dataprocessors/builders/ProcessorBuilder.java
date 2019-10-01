package com.bbva.dataprocessors.builders;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Processor builder interface
 */
public interface ProcessorBuilder {

    /**
     * Build
     */
    void build();

    /**
     * Start the builder
     */
    void start();

    /**
     * Get streams
     *
     * @return streams instance
     */
    KafkaStreams streams();

    /**
     * Close the builder and process
     */
    void close();
}
