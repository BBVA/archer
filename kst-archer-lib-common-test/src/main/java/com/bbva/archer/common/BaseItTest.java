package com.bbva.archer.common;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.config.Config;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.eclipse.jetty.server.Server;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;

@Config(file = "app.yml")
public class BaseItTest {

    private static final LoggerGen LOGGER = LoggerGenesis.getLogger(BaseItTest.class.getName());
    private static final int DEFAULT_SCHEMA_REGISTRY_PORT = 8081;

    @ClassRule
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

    protected static ApplicationConfig appConfig;

    @BeforeClass
    public static void setUpKafka() {
        final int schemaRegistryPort = getSchemaRegistryPort();
        final Properties defaultConfig = new Properties();
        defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:" + schemaRegistryPort);

        final String bootstrapServer = sharedKafkaTestResource.getKafkaConnectString();

        appConfig = (new AppConfiguration()).init(BaseItTest.class.getAnnotation(Config.class));

        appConfig.put("schema.registry.url", "http://localhost:" + schemaRegistryPort);
        appConfig.put("bootstrap.servers", bootstrapServer);
        appConfig.consumer().put("bootstrap.servers", bootstrapServer);
        appConfig.producer().put("bootstrap.servers", bootstrapServer);
        appConfig.streams().put("bootstrap.servers", bootstrapServer);

        try {
            defaultConfig.put("kafkastore.connection.url", sharedKafkaTestResource.getZookeeperConnectString());
            defaultConfig.put("kafkastore.bootstrap.servers", bootstrapServer);
            defaultConfig.put("kafkastore.group.id", "local-test-group");
            defaultConfig.put("kafkastore.security.protocol", "PLAINTEXT");
            defaultConfig.put("cluster.enable", "false");
            defaultConfig.put("port", schemaRegistryPort);

            final SchemaRegistryConfig configRegistry = new SchemaRegistryConfig(defaultConfig);
            final SchemaRegistryRestApplication app = new SchemaRegistryRestApplication(configRegistry);
            final Server server = app.createServer();

            server.start();
        } catch (final Exception e) {
            LOGGER.error("Error launching schema registry", e);
        }
    }

    private static int getSchemaRegistryPort() {
        try (final ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (final IOException e) {
            return DEFAULT_SCHEMA_REGISTRY_PORT;
        }
    }
}
