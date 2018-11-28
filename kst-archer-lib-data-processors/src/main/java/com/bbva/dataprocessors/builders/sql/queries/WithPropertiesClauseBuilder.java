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

    private Map<String, String> withProperties = new HashMap<>();

    public String kafkaTopic() {
        return withProperties.get(KAFKA_TOPIC);
    }

    public WithPropertiesClauseBuilder kafkaTopic(String kafkaTopic) {
        withProperties.put(KAFKA_TOPIC, kafkaTopic);
        return this;
    }

    public String valueFormat() {
        return withProperties.get(VALUE_FORMAT);
    }

    public WithPropertiesClauseBuilder valueFormat(String valueFormat) {
        withProperties.put(VALUE_FORMAT, valueFormat);
        return this;
    }

    public String key() {
        return withProperties.get(KEY);
    }

    public WithPropertiesClauseBuilder key(String key) {
        withProperties.put(KEY, key);
        return this;
    }

    public String timestamp() {
        return withProperties.get(TIMESTAMP);
    }

    public WithPropertiesClauseBuilder timestamp(String timestamp) {
        withProperties.put(TIMESTAMP, timestamp);
        return this;
    }

    public String timestampFormat() {
        return withProperties.get(TIMESTAMP_FORMAT);
    }

    public WithPropertiesClauseBuilder timestampFormat(String timestampFormat) {
        withProperties.put(TIMESTAMP_FORMAT, timestampFormat);
        return this;
    }

    public String partitions() {
        return withProperties.get(PARTITIONS);
    }

    public WithPropertiesClauseBuilder partitions(String partitions) {
        withProperties.put(PARTITIONS, partitions);
        return this;
    }

    public String replicas() {
        return withProperties.get(REPLICAS);
    }

    public WithPropertiesClauseBuilder replicas(String replicas) {
        withProperties.put(REPLICAS, replicas);
        return this;
    }

    String build() {
        String withClause = "";
        if (!withProperties.isEmpty()) {
            List<String> withPropertiesList = new ArrayList<>();
            for (String prop : withProperties.keySet()) {
                withPropertiesList.add(prop.toUpperCase() + " = '" + withProperties.get(prop) + "'");
            }
            withClause = " WITH (" + String.join(", ", withPropertiesList) + ")";
        }
        return withClause;
    }
}
