package com.bbva.dataprocessors.contexts.dataflow;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.CustomCachedSchemaRegistryClient;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class DataflowProcessorContextSupplier implements DataflowProcessorContext {

    private ApplicationConfig config;
    private final StreamsBuilder builder;
    private final CustomCachedSchemaRegistryClient schemaRegistry;
    private final Map<String, String> serdeProps;
    private String name;

    public DataflowProcessorContextSupplier(String name, ApplicationConfig config) {
        this.name = name;

        String clientId = UUID.randomUUID().toString();
        this.config = new ApplicationConfig();
        this.config.put(config.get());
        this.config.consumer().put(config.consumer().get());
        this.config.producer().put(config.producer().get());
        this.config.streams().put(config.streams().get());

        this.config.streams().put(ApplicationConfig.StreamsProperties.MAX_POLL_INTERVAL_MS, 300000);
        this.config.streams().put(ApplicationConfig.StreamsProperties.MAX_POLL_RECORDS, 500);
        this.config.streams().put(ApplicationConfig.StreamsProperties.SESSION_TIMEOUT_MS, 10000);

        Object stateDir = config.streams().get(ApplicationConfig.StreamsProperties.STATE_DIR);
        this.config.streams().put(ApplicationConfig.StreamsProperties.STATE_DIR,
                (stateDir != null ? stateDir.toString() : "/tmp/kafka-streams-") + clientId);

        String applicationName = config.streams().get(ApplicationConfig.StreamsProperties.APPLICATION_NAME).toString();
        this.config.streams().put(ApplicationConfig.StreamsProperties.APPLICATION_ID, applicationName + "_" + name);

        config.streams().put(ApplicationConfig.StreamsProperties.STREAMS_CLIENT_ID, clientId);

        final String schemaRegistryUrl = this.config.get(ApplicationConfig.SCHEMA_REGISTRY_URL).toString();

        schemaRegistry = new CustomCachedSchemaRegistryClient(schemaRegistryUrl, 100);

        serdeProps = Collections.singletonMap(ApplicationConfig.SCHEMA_REGISTRY_URL, schemaRegistryUrl);

        builder = new StreamsBuilder();
    }

    public CustomCachedSchemaRegistryClient schemaRegistryClient() {
        return schemaRegistry;
    };

    public Map<String, String> serdeProperties() {
        return serdeProps;
    }

    public ApplicationConfig configs() {
        return config;
    }

    public StreamsBuilder streamsBuilder() {
        return builder;
    }

    public String name() {
        return name;
    }

    public String applicationId() {
        return config.streams().get(ApplicationConfig.StreamsProperties.APPLICATION_ID).toString();
    }

}
