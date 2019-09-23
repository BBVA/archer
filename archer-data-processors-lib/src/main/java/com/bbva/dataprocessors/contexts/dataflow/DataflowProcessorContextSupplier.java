package com.bbva.dataprocessors.contexts.dataflow;

import com.bbva.common.config.ApplicationConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Collections;
import java.util.Map;

/**
 * Dataflow processor context implementation
 */
public class DataflowProcessorContextSupplier implements DataflowProcessorContext {

    private final ApplicationConfig config;
    private final StreamsBuilder builder;
    private final CachedSchemaRegistryClient schemaRegistry;
    private final Map<String, String> serdeProps;
    private final String name;

    /**
     * Constructor
     *
     * @param name   processor name
     * @param config application configuration
     */
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

        schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        serdeProps = Collections.singletonMap(ApplicationConfig.SCHEMA_REGISTRY_URL, schemaRegistryUrl);

        builder = new StreamsBuilder();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CachedSchemaRegistryClient schemaRegistryClient() {
        return schemaRegistry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> serdeProperties() {
        return serdeProps;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ApplicationConfig configs() {
        return config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamsBuilder streamsBuilder() {
        return builder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String applicationId() {
        return config.streams().get(ApplicationConfig.StreamsProperties.APPLICATION_ID).toString();
    }

}
