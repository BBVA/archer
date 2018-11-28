package com.bbva.dataprocessors.builders;

import org.apache.kafka.streams.KafkaStreams;

public interface ProcessorBuilder {

    void build();

    void start();

    KafkaStreams streams();

    void close();
}
