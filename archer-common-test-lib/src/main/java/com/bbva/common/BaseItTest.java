package com.bbva.common;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.config.Config;
import com.bbva.common.util.KafkaTestResource;
import com.bbva.common.util.TestUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.BeforeClass;
import org.junit.ClassRule;

@Config(file = "app.yml")
public class BaseItTest {
    private static final int DEFAULT_SCHEMA_REGISTRY_PORT = 8081;

    @ClassRule
    public static final KafkaTestResource kafkaTestResource = new KafkaTestResource();

    @BeforeClass
    public static void setUpKafka() {
        final int schemaRegistryPort = TestUtil.getFreePort(DEFAULT_SCHEMA_REGISTRY_PORT);
        final String bootstrapServer = kafkaTestResource.getKafkaConnectString();
        final ApplicationConfig appConfig = new AppConfiguration().init(BaseItTest.class.getAnnotation(Config.class));

        appConfig.put(ApplicationConfig.SCHEMA_REGISTRY_URL, KafkaTestResource.HTTP_LOCALHOST + schemaRegistryPort);
        appConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        appConfig.consumer().put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        appConfig.producer().put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        appConfig.streams().put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        appConfig.ksql().put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        appConfig.dataflow().put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        kafkaTestResource.initSchemaRegistry(schemaRegistryPort);
    }

}
