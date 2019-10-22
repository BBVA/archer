package com.bbva.dataprocessors.builders.sql.queries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * With properties clause builder
 */
public class WithPropertiesClauseBuilder {

    private static final String KAFKA_TOPIC = "KAFKA_TOPIC";
    private static final String VALUE_FORMAT = "VALUE_FORMAT";
    private static final String KEY = "KEY";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String TIMESTAMP_FORMAT = "TIMESTAMP_FORMAT";
    private static final String PARTITIONS = "PARTITIONS";
    private static final String REPLICAS = "REPLICAS";

    public static final String AVRO_FORMAT = "AVRO";
    public static final String JSON_FORMAT = "JSON";
    public static final String DELIMITED_FORMAT = "DELIMITED";

    private final Map<String, Object> withProperties = new HashMap<>();

    /**
     * Get the kafka topic
     *
     * @return topic
     */
    public String kafkaTopic() {
        return (String) withProperties.get(KAFKA_TOPIC);
    }

    /**
     * Set kafka topic
     *
     * @param kafkaTopic topic
     * @return builder
     */
    public WithPropertiesClauseBuilder kafkaTopic(final String kafkaTopic) {
        withProperties.put(KAFKA_TOPIC, kafkaTopic);
        return this;
    }

    /**
     * Get value format property
     *
     * @return value format
     */
    public String valueFormat() {
        return (String) withProperties.get(VALUE_FORMAT);
    }

    /**
     * Set value format
     *
     * @param valueFormat value format
     * @return builder
     */
    public WithPropertiesClauseBuilder valueFormat(final String valueFormat) {
        withProperties.put(VALUE_FORMAT, valueFormat);
        return this;
    }

    /**
     * Get key property
     *
     * @return key
     */
    public String key() {
        return (String) withProperties.get(KEY);
    }

    /**
     * Set key property
     *
     * @param key key to set
     * @return builder
     */
    public WithPropertiesClauseBuilder key(final String key) {
        withProperties.put(KEY, key);
        return this;
    }

    /**
     * Get timestamp
     *
     * @return timestamp
     */
    public Long timestamp() {
        return (long) withProperties.get(TIMESTAMP);
    }

    /**
     * Set timestamp property
     *
     * @param timestamp timestamp
     * @return builder
     */
    public WithPropertiesClauseBuilder timestamp(final Long timestamp) {
        withProperties.put(TIMESTAMP, timestamp);
        return this;
    }

    /**
     * Get timestamp format
     *
     * @return the property
     */
    public String timestampFormat() {
        return String.valueOf(withProperties.get(TIMESTAMP_FORMAT));
    }

    /**
     * Set timestamp format
     *
     * @param timestampFormat format
     * @return builder
     */
    public WithPropertiesClauseBuilder timestampFormat(final String timestampFormat) {
        withProperties.put(TIMESTAMP_FORMAT, timestampFormat);
        return this;
    }

    /**
     * get partitions
     *
     * @return partitions
     */
    public Integer partitions() {
        return (int) withProperties.get(PARTITIONS);
    }

    /**
     * Set partitions
     *
     * @param partitions partitions number
     * @return builder
     */
    public WithPropertiesClauseBuilder partitions(final Integer partitions) {
        withProperties.put(PARTITIONS, partitions);
        return this;
    }

    /**
     * Get replication number
     *
     * @return number of replicas
     */
    public Integer replicas() {
        return (int) withProperties.get(REPLICAS);
    }

    /**
     * Set number of replicas
     *
     * @param replicas number of replicas
     * @return builder
     */
    public WithPropertiesClauseBuilder replicas(final Integer replicas) {
        withProperties.put(REPLICAS, replicas);
        return this;
    }

    /**
     * Build the query
     *
     * @return
     */
    String build() {
        String withClause = "";
        if (!withProperties.isEmpty()) {
            final List<String> withPropertiesList = new ArrayList<>();
            for (final String prop : withProperties.keySet()) {
                if (withProperties.get(prop) instanceof String) {
                    withPropertiesList.add(prop.toUpperCase() + " = '" + withProperties.get(prop) + "'");
                } else {
                    withPropertiesList.add(prop.toUpperCase() + " = " + withProperties.get(prop));
                }

            }
            withClause = String.format(" WITH ( %s )", String.join(", ", withPropertiesList));
        }
        return withClause;
    }
}
