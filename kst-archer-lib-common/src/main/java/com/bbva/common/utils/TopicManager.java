package com.bbva.common.utils;

import com.bbva.common.config.ApplicationConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.log4j.Logger;

import java.util.*;

public class TopicManager {

    public static final int DEFAULT_PARTITIONS = 2;
    public static final short DEFAULT_REPLICATION = 3;

    private static final Logger logger = Logger.getLogger(TopicManager.class);

    public static void createTopics(String topicName, Map<String, String> topicConfig, ApplicationConfig config) {
        NewTopic newTopic = new NewTopic(topicName, getPartitions(config), getReplicationFactor(config));
        if (!topicConfig.isEmpty()) {
            newTopic.configs(topicConfig);
        }
        createAdminClient(Arrays.asList(newTopic), config);
    }

    public static void createTopics(Collection<String> topicNames, ApplicationConfig config) {
        Collection<NewTopic> topics = new ArrayList<>();
        NewTopic newTopic;
        for (String topicName : topicNames) {
            newTopic = new NewTopic(topicName, getPartitions(config), getReplicationFactor(config));
            topics.add(newTopic);
        }
        logger.debug("Create topics " + Arrays.toString(topicNames.toArray()));
        createAdminClient(topics, config);
    }

    public static void createTopics(Map<String, Map<String, String>> topicNames, ApplicationConfig config) {
        Collection<NewTopic> topics = new ArrayList<>();
        NewTopic newTopic;
        for (String topicName : topicNames.keySet()) {
            newTopic = new NewTopic(topicName, getPartitions(config), getReplicationFactor(config));
            Map<String, String> newTopicConfig = topicNames.get(topicName);
            if (!newTopicConfig.isEmpty()) {
                newTopic.configs(newTopicConfig);
            }
            topics.add(newTopic);
        }
        logger.debug("Create topics " + Arrays.toString(topicNames.keySet().toArray()));
        createAdminClient(topics, config);
    }

    private static void createAdminClient(Collection<NewTopic> topics, ApplicationConfig config) {
        Properties props = new Properties();
        props.setProperty(ApplicationConfig.StreamsProperties.BOOTSTRAP_SERVERS,
                config.streams().get(ApplicationConfig.StreamsProperties.BOOTSTRAP_SERVERS).toString());
        AdminClient adminClient = AdminClient.create(props);
        adminClient.createTopics(topics);
        adminClient.close();
    }

    private static int getPartitions(ApplicationConfig config) {
        int partitions;
        try {
            partitions = new Integer(config.get(ApplicationConfig.PARTITIONS).toString());
        } catch (NullPointerException e) {
            partitions = TopicManager.DEFAULT_PARTITIONS;
        }
        return partitions;
    }

    private static short getReplicationFactor(ApplicationConfig config) {
        short replication;
        try {
            replication = new Integer(config.get(ApplicationConfig.REPLICATION_FACTOR).toString()).shortValue();
        } catch (NullPointerException e) {
            replication = TopicManager.DEFAULT_REPLICATION;
        }
        return replication;
    }

}
