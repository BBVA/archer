package com.bbva.dataprocessors.contexts.sql;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.util.KsqlConfig;

import java.util.Map;

/**
 * SQL processor context implementation
 */
public class SQLProcessorContextSupplier implements SQLProcessorContext {

    private static final Logger logger = LoggerFactory.getLogger(SQLProcessorContextSupplier.class);

    private final ApplicationConfig config;
    private final CachedSchemaRegistryClient schemaRegistry;
    private final String name;
    private final KsqlContext ksqlContext;

    /**
     * Constructor
     *
     * @param name   processor name
     * @param config configurations
     */
    public SQLProcessorContextSupplier(final String name, final ApplicationConfig config) {
        this.name = name;

        this.config = config;
        final String schemaRegistryUrl = this.config.get(ApplicationConfig.SCHEMA_REGISTRY_URL).toString();

        schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        ksqlContext = KsqlContext.create(new KsqlConfig(config.ksql().get()), ProcessingLogContext.create());
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
    public ApplicationConfig configs() {
        return config;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public KsqlContext ksqlContext() {
        return ksqlContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void printDataSources() {
        logger.info("KSQL DataSources:");
        logger.info("*****************");
        final Map<String, StructuredDataSource> metaStore = ksqlContext().getMetaStore().getAllStructuredDataSources();
        metaStore.forEach((key, dataSource) -> {
            logger.info("Data Source : {}", key);
            logger.info("-> Type: {}", dataSource.getDataSourceType());
            logger.info("-> Name: {}", dataSource.getName());
            logger.info("-> Topic: {}", dataSource.getTopicName());
            logger.info("-> Query ID: {}", dataSource.getPersistentQueryId().getId());
        });
    }
}
