package com.bbva.dataprocessors.contexts;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.CustomCachedSchemaRegistryClient;

public interface ProcessorContext {

    CustomCachedSchemaRegistryClient schemaRegistryClient();

    ApplicationConfig configs();

    String applicationId();

    String name();

}
