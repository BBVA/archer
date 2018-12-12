package com.bbva.dataprocessors.contexts.dataflow;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.CustomCachedSchemaRegistryClient;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Collections;
import java.util.Map;

public class DataflowProcessorContextSupplier implements DataflowProcessorContext {

    private final ApplicationConfig config;
    private final StreamsBuilder builder;
    private final CustomCachedSchemaRegistryClient schemaRegistry;
    private final Map<String, String> serdeProps;
    private final String name;

    public DataflowProcessorContextSupplier(final String name, final ApplicationConfig config) {
        this.name = name;

        this.config = new ApplicationConfig();
        this.config.put(config.get());
        this.config.consumer().put(config.consumer().get());
        this.config.producer().put(config.producer().get());
        this.config.streams().put(config.streams().get());
        this.config.streams().get().putAll(config.dataflow().get());

        final String applicationName = config.streams().get(ApplicationConfig.StreamsProperties.APPLICATION_NAME).toString();
        this.config.streams().put(ApplicationConfig.StreamsProperties.APPLICATION_ID, applicationName + "_" + name);

        final String schemaRegistryUrl = this.config.get(ApplicationConfig.SCHEMA_REGISTRY_URL).toString();

        schemaRegistry = new CustomCachedSchemaRegistryClient(schemaRegistryUrl, 100);

        serdeProps = Collections.singletonMap(ApplicationConfig.SCHEMA_REGISTRY_URL, schemaRegistryUrl);

        builder = new StreamsBuilder();
    }

    @Override
    public CustomCachedSchemaRegistryClient schemaRegistryClient() {
        return schemaRegistry;
    }

    @Override
    public Map<String, String> serdeProperties() {
        return serdeProps;
    }

    @Override
    public ApplicationConfig configs() {
        return config;
    }

    @Override
    public StreamsBuilder streamsBuilder() {
        return builder;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String applicationId() {
        return config.streams().get(ApplicationConfig.StreamsProperties.APPLICATION_ID).toString();
    }

}
