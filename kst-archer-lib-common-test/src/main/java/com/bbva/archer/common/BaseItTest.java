package com.bbva.archer.common;

import com.bbva.archer.common.util.KafkaTestResource;
import com.bbva.archer.common.util.TestUtil;
import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.config.Config;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.PRecord;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.RecordHeaders;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.UUID;

@Config(file = "app.yml")
public class BaseItTest {
    private static final int DEFAULT_SCHEMA_REGISTRY_PORT = 8081;

    @ClassRule
    public static final KafkaTestResource kafkaTestResource = new KafkaTestResource();

    protected static ApplicationConfig appConfig;
    private static CachedProducer producer;

    @BeforeClass
    public static void setUpKafka() {
        final int schemaRegistryPort = TestUtil.getFreePort(DEFAULT_SCHEMA_REGISTRY_PORT);
        final String bootstrapServer = kafkaTestResource.getKafkaConnectString();
        appConfig = new AppConfiguration().init(BaseItTest.class.getAnnotation(Config.class));

        appConfig.put(ApplicationConfig.SCHEMA_REGISTRY_URL, KafkaTestResource.HTTP_LOCALHOST + schemaRegistryPort);
        appConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        appConfig.consumer().put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        appConfig.producer().put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        appConfig.streams().put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        kafkaTestResource.initSchemaRegistry(schemaRegistryPort);
        producer = new CachedProducer(appConfig);
    }

    public static <T extends SpecificRecord> void generateTestEvent(final String topic, final T record) {
        final String key = UUID.randomUUID().toString();
        producer.add(new PRecord<>(topic, key, record,
                generateHeaders(key)), BaseItTest::handlePutRecord);
    }


    private static RecordHeaders generateHeaders(final String key) {

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("action", new ByteArrayValue("command"));
        recordHeaders.add("key", new ByteArrayValue(key));
        recordHeaders.add(CRecord.FLAG_REPLAY_KEY, new ByteArrayValue(false));

        return recordHeaders;
    }

    protected static void handlePutRecord(final Object record, final Exception exception) {
    }
}
