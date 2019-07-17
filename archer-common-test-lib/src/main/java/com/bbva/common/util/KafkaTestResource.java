package com.bbva.common.util;


import com.bbva.common.BaseItTest;
import com.salesforce.kafka.test.AbstractKafkaTestResource;
import com.salesforce.kafka.test.KafkaBroker;
import com.salesforce.kafka.test.KafkaTestCluster;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.eclipse.jetty.server.Server;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;


public class KafkaTestResource extends AbstractKafkaTestResource<SharedKafkaTestResource> implements TestRule {
    private static final Logger logger = LoggerFactory.getLogger(BaseItTest.class);
    public static final String HTTP_LOCALHOST = "http://localhost:";
    private static Server server;

    private static SchemaRegistryRestApplication app;

    private void before() throws Exception {
        logger.info("Starting kafka test server");

        if (getKafkaCluster() != null) {
            throw new IllegalStateException("Unknown State!  Kafka Test Server already exists!");
        } else {
            setKafkaCluster(new KafkaTestCluster(getNumberOfBrokers(), getBrokerProperties(), Collections.singletonList(getRegisteredListener())));
            getKafkaCluster().start();
        }
    }

    public void initSchemaRegistry(final int schemaRegistryPort) {

        final Properties defaultConfig = new Properties();
        defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, HTTP_LOCALHOST + schemaRegistryPort);
        try {
            defaultConfig.put("kafkastore.connection.url", getZookeeperConnectString());
            defaultConfig.put("kafkastore.bootstrap.servers", getKafkaConnectString());
            defaultConfig.put("kafkastore.group.id", "local-test-group");
            defaultConfig.put("kafkastore.security.protocol", "PLAINTEXT");
            defaultConfig.put("cluster.enable", "true");
            defaultConfig.put("port", schemaRegistryPort);

            final SchemaRegistryConfig configRegistry = new SchemaRegistryConfig(defaultConfig);
            app = new SchemaRegistryRestApplication(configRegistry);
            server = app.createServer();

            server.start();
        } catch (final Exception e) {
            logger.error("Error launching schema registry", e);
        }
    }

    private void after() throws Exception {
        logger.info("Shutting down kafka test server");
        app.stop();
        server.stop();

        final List<KafkaBroker> brokers = getKafkaBrokers().asList();
        for (final KafkaBroker broker : brokers) {
            broker.stop();
        }

        getKafkaTestUtils().getAdminClient().close();

        if (getKafkaCluster() != null) {
            try {
                getKafkaCluster().close();
            } catch (final Exception var2) {
                throw new RuntimeException(var2);
            }

            setKafkaCluster(null);
        }
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                before();
                try {
                    base.evaluate();
                } finally {
                    after();
                }

            }
        };
    }


}
