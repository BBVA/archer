package com.bbva.dataprocessors.contexts;

import com.bbva.common.config.ApplicationConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;

public interface ProcessorContext {

    CachedSchemaRegistryClient schemaRegistryClient();

    ApplicationConfig configs();

    String applicationId();

    String name();

}
