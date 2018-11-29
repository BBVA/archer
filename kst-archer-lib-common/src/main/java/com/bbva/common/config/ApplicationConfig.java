package com.bbva.common.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class ApplicationConfig implements Cloneable {

    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String REPLAY_TOPICS = "replay.topics";
    public static final String REPLICATION_FACTOR = "replication.factor";
    public static final String PARTITIONS = "partitions";

    public static final String CHANGELOG_RECORD_NAME_SUFFIX = "_data_changelog";
    public static final String COMMANDS_RECORD_NAME_SUFFIX = "_commands";
    public static final String TRANSACTIONS_RECORD_NAME_SUFFIX = "_transaction_log";
    public static final String EVENTS_RECORD_NAME_SUFFIX = "_events";
    public static final String SNAPSHOT_RECORD_NAME_SUFFIX = "";
    public static final String STORE_NAME_SUFFIX = "_store";
    public static final String GLOBAL_NAME_PREFIX = "global_";
    public static final String INTERNAL_NAME_PREFIX = "internal_";
    public static final String KSQL_PREFIX = "ksql_";
    public static final String KSQL_SUFFIX = "_ksql";
    public static final String KSQL_TABLE_SUFFIX = KSQL_SUFFIX + "_table";
    public static final String KSQL_STREAM_SUFFIX = KSQL_SUFFIX + "_stream";

    private Properties applicationProperties = new Properties();
    private ProducerProperties producerProperties = new ProducerProperties();
    private ConsumerProperties consumerProperties = new ConsumerProperties();
    private StreamsProperties streamsProperties = new StreamsProperties();

    public <V> void put(Properties applicationProperties) {
        this.applicationProperties = (Properties) applicationProperties.clone();
    }

    public <V> void put(String key, V value) {
        applicationProperties.put(key, value);
    }

    public Object get(String key) {
        return applicationProperties.getProperty(key);
    }

    public Properties get() {
        return applicationProperties;
    }

    public ProducerProperties producer() {
        return producerProperties;
    }

    public ConsumerProperties consumer() {
        return consumerProperties;
    }

    public StreamsProperties streams() {
        return streamsProperties;
    }

    final public class ProducerProperties {

        public static final String BOOTSTRAP_SERVERS = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        public static final String PRODUCER_ACKS = ProducerConfig.ACKS_CONFIG;
        public static final String PRODUCER_RETRIES = ProducerConfig.RETRIES_CONFIG;
        public static final String INTERCEPTOR_CLASSES = ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;

        private Properties producerProperties = new Properties();

        public <V> void put(Properties producerProperties) {
            this.producerProperties = (Properties) producerProperties.clone();
        }

        public <V> void put(String key, V value) {
            producerProperties.put(key, value);
        }

        public Properties get() {
            return producerProperties;
        }

        public Object get(String property) {
            return producerProperties.get(property);
        }

    }

    final public class ConsumerProperties {

        public static final String BOOTSTRAP_SERVERS = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        public static final String CONSUMER_GROUP_ID = ConsumerConfig.GROUP_ID_CONFIG;
        public static final String CLIENT_ID = ConsumerConfig.CLIENT_ID_CONFIG;
        public static final String AUTO_OFFSET_RESET = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
        public static final String ENABLE_AUTO_COMMIT = ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
        public static final String AUTO_COMMIT_INTERVAL_MS = ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
        public static final String SESSION_TIMEOUT_MS = ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
        public static final String INTERCEPTOR_CLASSES = ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;

        private Properties consumerProperties = new Properties();

        public <V> void put(Properties consumerProperties) {
            this.consumerProperties = (Properties) consumerProperties.clone();
        }

        public <V> void put(String key, V value) {
            consumerProperties.put(key, value);
        }

        public Properties get() {
            return consumerProperties;
        }

        public Object get(String property) {
            return consumerProperties.get(property);
        }

    }

    final public class StreamsProperties {

        public static final String BOOTSTRAP_SERVERS = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        public static final String CONSUMER_GROUP_ID = ConsumerConfig.GROUP_ID_CONFIG;
        public static final String APPLICATION_ID = StreamsConfig.APPLICATION_ID_CONFIG;
        public static final String APPLICATION_NAME = "application.name";
        public static final String APPLICATION_SERVER = StreamsConfig.APPLICATION_SERVER_CONFIG;
        public static final String STREAMS_CLIENT_ID = ConsumerConfig.CLIENT_ID_CONFIG;
        public static final String STREAMS_AUTO_OFFSET_RESET = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
        public static final String COMMIT_INTERVAL_MS = StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
        public static final String INTERNAL_REPLICATION_FACTOR = StreamsConfig.REPLICATION_FACTOR_CONFIG;
        public static final String MAX_POLL_RECORDS = ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
        public static final String MAX_POLL_INTERVAL_MS = ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
        public static final String SESSION_TIMEOUT_MS = ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
        public static final String STATE_DIR = StreamsConfig.STATE_DIR_CONFIG;
        public static final String PRODUCER_INTERCEPTOR_CLASSES = StreamsConfig.PRODUCER_PREFIX
                + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;
        public static final String CONSUMER_INTERCEPTOR_CLASSES = StreamsConfig.CONSUMER_PREFIX
                + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;

        private Properties streamsProperties = new Properties();

        public <V> void put(Properties streamsProperties) {
            this.streamsProperties = (Properties) streamsProperties.clone();
        }

        public <V> void put(String key, V value) {
            streamsProperties.put(key, value);
        }

        public Properties get() {
            return streamsProperties;
        }

        public Object get(String property) {
            return streamsProperties.get(property);
        }

    }
}
