package com.bbva.dataprocessors.contexts.dataflow;

import com.bbva.common.config.AppConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Collections;
import java.util.Map;

/**
 * Dataflow processor context implementation
 */
public class DataflowProcessorContextSupplier implements DataflowProcessorContext {

    private final AppConfig config;
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
    public DataflowProcessorContextSupplier(final String name, final AppConfig config) {
        this.name = name;

        this.config = new AppConfig();
        this.config.put(config.get());
        this.config.consumer().putAll(config.consumer());
        this.config.producer().putAll(config.producer());
        this.config.streams().putAll(config.streams());
        this.config.dataflow().putAll(config.dataflow());

        final String applicationName = config.streams(AppConfig.StreamsProperties.APPLICATION_NAME).toString();
        this.config.streams().put(AppConfig.StreamsProperties.APPLICATION_ID, applicationName + "_" + name);

        final String schemaRegistryUrl = this.config.get(AppConfig.SCHEMA_REGISTRY_URL).toString();

        schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        serdeProps = Collections.singletonMap(AppConfig.SCHEMA_REGISTRY_URL, schemaRegistryUrl);

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
    public AppConfig configs() {
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
        return config.streams(AppConfig.StreamsProperties.APPLICATION_ID).toString();
    }

}
