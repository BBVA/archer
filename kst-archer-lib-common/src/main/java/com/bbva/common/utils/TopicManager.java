package com.bbva.common.utils;

import com.bbva.common.config.ApplicationConfig;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.*;

public class TopicManager {

    public static final int DEFAULT_PARTITIONS = 2;
    public static final short DEFAULT_REPLICATION = 3;

    private static final LoggerGen logger = LoggerGenesis.getLogger(TopicManager.class.getName());

    public static void createTopics(final String topicName, final Map<String, String> topicConfig,
            final ApplicationConfig config) {
        final NewTopic newTopic = new NewTopic(topicName, getPartitions(config), getReplicationFactor(config));
        if (!topicConfig.isEmpty()) {
            newTopic.configs(topicConfig);
        }
        createAdminClient(Arrays.asList(newTopic), config);
    }

    public static void createTopics(final Collection<String> topicNames, final ApplicationConfig config) {
        final Collection<NewTopic> topics = new ArrayList<>();
        NewTopic newTopic;
        for (final String topicName : topicNames) {
            newTopic = new NewTopic(topicName, getPartitions(config), getReplicationFactor(config));
            topics.add(newTopic);
        }
        logger.debug("Create topics " + Arrays.toString(topicNames.toArray()));
        createAdminClient(topics, config);
    }

    public static void createTopics(final Map<String, Map<String, String>> topicNames, final ApplicationConfig config) {
        final Collection<NewTopic> topics = new ArrayList<>();
        NewTopic newTopic;
        for (final String topicName : topicNames.keySet()) {
            newTopic = new NewTopic(topicName, getPartitions(config), getReplicationFactor(config));
            final Map<String, String> newTopicConfig = topicNames.get(topicName);
            if (!newTopicConfig.isEmpty()) {
                newTopic.configs(newTopicConfig);
            }
            topics.add(newTopic);
        }
        logger.debug("Create topics " + Arrays.toString(topicNames.keySet().toArray()));
        createAdminClient(topics, config);
    }

    private static void createAdminClient(final Collection<NewTopic> topics, final ApplicationConfig config) {
        final Properties props = new Properties();
        props.setProperty(ApplicationConfig.StreamsProperties.BOOTSTRAP_SERVERS,
                config.streams().get(ApplicationConfig.StreamsProperties.BOOTSTRAP_SERVERS).toString());
        final AdminClient adminClient = AdminClient.create(props);
        adminClient.createTopics(topics);
        adminClient.close();
    }

    private static int getPartitions(final ApplicationConfig config) {
        int partitions;
        try {
            partitions = new Integer(config.get(ApplicationConfig.PARTITIONS).toString());
        } catch (final NullPointerException e) {
            partitions = TopicManager.DEFAULT_PARTITIONS;
        }
        return partitions;
    }

    private static short getReplicationFactor(final ApplicationConfig config) {
        short replication;
        try {
            replication = new Integer(config.get(ApplicationConfig.REPLICATION_FACTOR).toString()).shortValue();
        } catch (final NullPointerException e) {
            replication = TopicManager.DEFAULT_REPLICATION;
        }
        return replication;
    }
}
