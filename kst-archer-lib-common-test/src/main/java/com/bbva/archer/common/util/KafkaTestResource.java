package com.bbva.archer.common.util;


import com.bbva.archer.common.BaseItTest;
import com.salesforce.kafka.test.AbstractKafkaTestResource;
import com.salesforce.kafka.test.KafkaBroker;
import com.salesforce.kafka.test.KafkaCluster;
import com.salesforce.kafka.test.KafkaTestCluster;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.eclipse.jetty.server.Server;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Collections;
import java.util.List;
import java.util.Properties;


public class KafkaTestResource extends AbstractKafkaTestResource<SharedKafkaTestResource> implements TestRule {
    private static final LoggerGen LOGGER = LoggerGenesis.getLogger(BaseItTest.class.getName());
    public static final String HTTP_LOCALHOST = "http://localhost:";
    private static Server server;

    private static SchemaRegistryRestApplication app;

    private void before() throws Exception {
        LOGGER.info("Starting kafka test server");
        if (this.getKafkaCluster() != null) {
            throw new IllegalStateException("Unknown State!  Kafka Test Server already exists!");
        } else {
            this.setKafkaCluster(new KafkaTestCluster(this.getNumberOfBrokers(), this.getBrokerProperties(), Collections.singletonList(this.getRegisteredListener())));
            this.getKafkaCluster().start();
        }
    }

    public void initSchemaRegistry(final int schemaRegistryPort) {
        final Properties defaultConfig = new Properties();
        defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, HTTP_LOCALHOST + schemaRegistryPort);
        try {
            defaultConfig.put("kafkastore.connection.url", this.getZookeeperConnectString());
            defaultConfig.put("kafkastore.bootstrap.servers", this.getKafkaConnectString());
            defaultConfig.put("kafkastore.group.id", "local-test-group");
            defaultConfig.put("kafkastore.security.protocol", "PLAINTEXT");
            defaultConfig.put("cluster.enable", "true");
            defaultConfig.put("port", schemaRegistryPort);

            final SchemaRegistryConfig configRegistry = new SchemaRegistryConfig(defaultConfig);
            app = new SchemaRegistryRestApplication(configRegistry);
            server = app.createServer();

            server.start();
        } catch (final Exception e) {
            LOGGER.error("Error launching schema registry", e);
        }
    }

    private void after() throws Exception {
        LOGGER.info("Shutting down kafka test server");
        app.stop();
        server.stop();

        final List<KafkaBroker> brokers = this.getKafkaBrokers().asList();
        for (final KafkaBroker broker : brokers) {
            broker.stop();
        }
        this.getKafkaTestUtils().getAdminClient().close();

        if (this.getKafkaCluster() != null) {
            try {
                this.getKafkaCluster().close();
            } catch (final Exception var2) {
                throw new RuntimeException(var2);
            }

            this.setKafkaCluster((KafkaCluster) null);
            this.before();
        }
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                KafkaTestResource.this.before();

                try {
                    base.evaluate();
                } finally {
                    KafkaTestResource.this.after();
                }

            }
        };
    }


}
