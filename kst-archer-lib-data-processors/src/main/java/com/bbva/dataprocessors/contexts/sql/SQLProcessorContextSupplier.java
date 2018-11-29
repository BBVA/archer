package com.bbva.dataprocessors.contexts.sql;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.CustomCachedSchemaRegistryClient;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.util.KsqlConfig;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class SQLProcessorContextSupplier implements SQLProcessorContext {

    private ApplicationConfig config;
    private final CustomCachedSchemaRegistryClient schemaRegistry;
    private final String name;
    private final KsqlContext ksqlContext;
    private static final LoggerGen logger = LoggerGenesis.getLogger(SQLProcessorContextSupplier.class.getName());

    public SQLProcessorContextSupplier(String name, ApplicationConfig config) {

        this.name = name;

        String clientId = UUID.randomUUID().toString();
        this.config = config;

        final String schemaRegistryUrl = this.config.get(ApplicationConfig.SCHEMA_REGISTRY_URL).toString();

        schemaRegistry = new CustomCachedSchemaRegistryClient(schemaRegistryUrl, 100);

        Properties ksqlConfig = new Properties();

        ksqlConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                this.config.streams().get(ApplicationConfig.StreamsProperties.BOOTSTRAP_SERVERS));
        ksqlConfig.put(KsqlConfig.KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG, "_store");
        ksqlConfig.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, this.config.get(ApplicationConfig.PARTITIONS));
        ksqlConfig.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY,
                this.config.get(ApplicationConfig.REPLICATION_FACTOR));
        ksqlConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        ksqlConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ksqlContext = KsqlContext.create(new KsqlConfig(ksqlConfig), schemaRegistry);
    }

    public CustomCachedSchemaRegistryClient schemaRegistryClient() {
        return schemaRegistry;
    };

    public ApplicationConfig configs() {
        return config;
    }

    public String name() {
        return name;
    }

    public String applicationId() {
        return config.streams().get(ApplicationConfig.StreamsProperties.APPLICATION_ID).toString();
    }

    public KsqlContext ksqlContext() {
        return ksqlContext;
    }

    public void printDataSources() {
        logger.info("KSQL DataSources:");
        logger.info("*****************");
        Map<String, StructuredDataSource> metaStore = ksqlContext().getMetaStore().getAllStructuredDataSources();
        metaStore.forEach((key, dataSource) -> {
            logger.info("Data Source : " + key);
            logger.info("-> Type: " + dataSource.getDataSourceType());
            logger.info("-> Name: " + dataSource.getName());
            logger.info("-> Topic: " + dataSource.getTopicName());
            logger.info("-> Query ID: " + dataSource.getPersistentQueryId().getId());
        });
    }
}
