package com.bbva.dataprocessors.builders.sql.queries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public String kafkaTopic() {
        return (String) withProperties.get(KAFKA_TOPIC);
    }

    public WithPropertiesClauseBuilder kafkaTopic(final String kafkaTopic) {
        withProperties.put(KAFKA_TOPIC, kafkaTopic);
        return this;
    }

    public String valueFormat() {
        return (String) withProperties.get(VALUE_FORMAT);
    }

    public WithPropertiesClauseBuilder valueFormat(final String valueFormat) {
        withProperties.put(VALUE_FORMAT, valueFormat);
        return this;
    }

    public String key() {
        return (String) withProperties.get(KEY);
    }

    public WithPropertiesClauseBuilder key(final String key) {
        withProperties.put(KEY, key);
        return this;
    }

    public Long timestamp() {
        return (long) withProperties.get(TIMESTAMP);
    }

    public WithPropertiesClauseBuilder timestamp(final Long timestamp) {
        withProperties.put(TIMESTAMP, timestamp);
        return this;
    }

    public String timestampFormat() {
        return String.valueOf(withProperties.get(TIMESTAMP_FORMAT));
    }

    public WithPropertiesClauseBuilder timestampFormat(final String timestampFormat) {
        withProperties.put(TIMESTAMP_FORMAT, timestampFormat);
        return this;
    }

    public Integer partitions() {
        return (int) withProperties.get(PARTITIONS);
    }

    public WithPropertiesClauseBuilder partitions(final Integer partitions) {
        withProperties.put(PARTITIONS, partitions);
        return this;
    }

    public Integer replicas() {
        return (int) withProperties.get(REPLICAS);
    }

    public WithPropertiesClauseBuilder replicas(final Integer replicas) {
        withProperties.put(REPLICAS, replicas);
        return this;
    }

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
            withClause = " WITH (" + String.join(", ", withPropertiesList) + ")";
        }
        return withClause;
    }
}
