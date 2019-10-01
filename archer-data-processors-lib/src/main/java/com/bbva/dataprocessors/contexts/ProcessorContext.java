package com.bbva.dataprocessors.contexts;

import com.bbva.common.config.ApplicationConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;

/**
 * Processor context interface
 */
public interface ProcessorContext {

    /**
     * Get shema registry client
     *
     * @return client
     */
    CachedSchemaRegistryClient schemaRegistryClient();

    /**
     * Get configs
     *
     * @return application config
     */
    ApplicationConfig configs();

    /**
     * Get application id
     *
     * @return application id
     */
    String applicationId();

    /**
     * get processor name
     *
     * @return name
     */
    String name();

}
