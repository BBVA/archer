package com.bbva.common.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class ApplicationConfig implements Cloneable {

    public final static String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public final static String REPLAY_TOPICS = "replay.topics";
    public final static String REPLICATION_FACTOR = "replication.factor";
    public final static String PARTITIONS = "partitions";

    public final static String CHANGELOG_RECORD_NAME_SUFFIX = "_data_changelog";
    public final static String COMMANDS_RECORD_NAME_SUFFIX = "_commands";
    public final static String TRANSACTIONS_RECORD_NAME_SUFFIX = "_transaction_log";
    public final static String EVENTS_RECORD_NAME_SUFFIX = "_events";
    public final static String SNAPSHOT_RECORD_NAME_SUFFIX = "";
    public final static String STORE_NAME_SUFFIX = "_store";
    public final static String GLOBAL_NAME_PREFIX = "global_";
    public final static String INTERNAL_NAME_PREFIX = "internal_";
    public final static String KSQL_PREFIX = "ksql_";
    public final static String KSQL_SUFFIX = "_ksql";
    public final static String KSQL_TABLE_SUFFIX = KSQL_SUFFIX + "_table";
    public final static String KSQL_STREAM_SUFFIX = KSQL_SUFFIX + "_stream";

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

        public final static String BOOTSTRAP_SERVERS = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        public final static String PRODUCER_ACKS = ProducerConfig.ACKS_CONFIG;
        public final static String PRODUCER_RETRIES = ProducerConfig.RETRIES_CONFIG;
        public final static String INTERCEPTOR_CLASSES = ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;

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

        public final static String BOOTSTRAP_SERVERS = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        public final static String CONSUMER_GROUP_ID = ConsumerConfig.GROUP_ID_CONFIG;
        public final static String CLIENT_ID = ConsumerConfig.CLIENT_ID_CONFIG;
        public final static String AUTO_OFFSET_RESET = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
        public final static String ENABLE_AUTO_COMMIT = ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
        public final static String AUTO_COMMIT_INTERVAL_MS = ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
        public final static String SESSION_TIMEOUT_MS = ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
        public final static String INTERCEPTOR_CLASSES = ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;

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

        public final static String BOOTSTRAP_SERVERS = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        public final static String CONSUMER_GROUP_ID = ConsumerConfig.GROUP_ID_CONFIG;
        public final static String APPLICATION_ID = StreamsConfig.APPLICATION_ID_CONFIG;
        public final static String APPLICATION_NAME = "application.name";
        public final static String APPLICATION_SERVER = StreamsConfig.APPLICATION_SERVER_CONFIG;
        public final static String STREAMS_CLIENT_ID = ConsumerConfig.CLIENT_ID_CONFIG;
        public final static String STREAMS_AUTO_OFFSET_RESET = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
        public final static String COMMIT_INTERVAL_MS = StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
        public final static String INTERNAL_REPLICATION_FACTOR = StreamsConfig.REPLICATION_FACTOR_CONFIG;
        public final static String MAX_POLL_RECORDS = ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
        public final static String MAX_POLL_INTERVAL_MS = ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
        public final static String SESSION_TIMEOUT_MS = ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
        public final static String STATE_DIR = StreamsConfig.STATE_DIR_CONFIG;
        public final static String PRODUCER_INTERCEPTOR_CLASSES = StreamsConfig.PRODUCER_PREFIX
                + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;
        public final static String CONSUMER_INTERCEPTOR_CLASSES = StreamsConfig.CONSUMER_PREFIX
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
