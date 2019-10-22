package com.bbva.common.utils;

import com.bbva.common.config.AppConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Topic creation with specific properties
 */
public class TopicManager {
    private static final Logger logger = LoggerFactory.getLogger(TopicManager.class);

    public static final int DEFAULT_PARTITIONS = 2;
    public static final int DEFAULT_REPLICATION = 3;

    public static final Map<String, Map<String, String>> configTypes;

    static {
        final Map<String, Map<String, String>> configMap = new HashMap<>();

        final Map<String, String> commandOrEventConfig = new HashMap<>();
        commandOrEventConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        commandOrEventConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1");
        commandOrEventConfig.put(TopicConfig.RETENTION_MS_CONFIG, "-1");
        configMap.put(AppConfig.EVENTS_RECORD_TYPE, commandOrEventConfig);
        configMap.put(AppConfig.COMMANDS_RECORD_TYPE, commandOrEventConfig);

        final Map<String, String> changelogConfig = new HashMap<>();
        changelogConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        changelogConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1");
        changelogConfig.put(TopicConfig.RETENTION_MS_CONFIG, "-1");
        configMap.put(AppConfig.CHANGELOG_RECORD_TYPE, changelogConfig);

        final Map<String, String> snapshotConfig = new HashMap<>();
        snapshotConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        snapshotConfig.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0");
        snapshotConfig.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0");
        configMap.put(AppConfig.SNAPSHOT_RECORD_TYPE, snapshotConfig);

        configMap.put(AppConfig.COMMON_RECORD_TYPE, Collections.emptyMap());

        configTypes = Collections.unmodifiableMap(configMap);
    }

    private TopicManager() {
        throw new UnsupportedOperationException();
    }


    /**
     * Create topics
     * For example:
     * <pre>
     *  {@code
     *      final Map<String, String> commandTopic = new HashMap<>();
     *      commandTopic.put("topic" + AppConfig.COMMANDS_RECORD_NAME_SUFFIX, AppConfig.COMMANDS_RECORD_TYPE);
     *      TopicManager.createTopics(commandTopic, appConfiguration);
     *  }
     * </pre>
     *
     * @param topicNames map with names and types
     * @param config     general configuration
     */
    public static void createTopics(
            final Map<String, String> topicNames, final AppConfig config) {
        final Collection<NewTopic> topics = new ArrayList<>();
        for (final String topicName : topicNames.keySet()) {
            topics.add(createTopic(config, topicName, configTypes.get(topicNames.get(topicName))));
        }
        logger.debug("Create topics {}", Arrays.toString(topicNames.keySet().toArray()));
        createTopics(topics, config);
    }

    /**
     * Create topics wit specific configurations
     *
     * @param topicNamesWithConfig map with topic names and configurations
     * @param config               general configuration
     */
    public static void createTopicsWithConfig(
            final Map<String, Map<String, String>> topicNamesWithConfig, final AppConfig config) {
        final Collection<NewTopic> topics = new ArrayList<>();
        for (final String topicName : topicNamesWithConfig.keySet()) {
            topics.add(createTopic(config, topicName, topicNamesWithConfig.get(topicName)));
        }
        logger.debug("Create topics {}", Arrays.toString(topicNamesWithConfig.keySet().toArray()));
        createTopics(topics, config);
    }

    private static NewTopic createTopic(
            final AppConfig config,
            final String topicName,
            final Map<String, String> topicConfig) {
        final NewTopic newTopic =
                new NewTopic(
                        topicName,
                        getProperty(config, AppConfig.PARTITIONS, DEFAULT_PARTITIONS),
                        (short) getProperty(config, AppConfig.REPLICATION_FACTOR, DEFAULT_REPLICATION));
        if (topicConfig != null && !topicConfig.isEmpty()) {
            newTopic.configs(topicConfig);
        }
        return newTopic;
    }

    private static void createTopics(
            final Collection<NewTopic> topics, final AppConfig config) {
        final AdminClient adminClient = AdminClient.create(config.get());
        adminClient.createTopics(topics);
        adminClient.close();
    }

    private static int getProperty(
            final AppConfig config, final String property, final int defaultValue) {
        return config.contains(property)
                ? Integer.valueOf(config.get(property).toString())
                : defaultValue;
    }
}
