package com.bbva.common.config.util;

import com.bbva.common.exceptions.ApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;

/**
 * Configuration utils
 */
public final class ConfigurationUtil {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationUtil.class);

    /**
     * Get all config properties of a yaml file
     *
     * @param yaml        yaml properties
     * @param classLoader class loader
     * @param filename    file
     * @return map of properties
     */
    public static Map<String, Object> getConfigFromFile(final Yaml yaml, final ClassLoader classLoader,
                                                        final String filename) {
        final Map<String, Object> properties;
        try (final InputStream in = classLoader.getResourceAsStream(filename)) {
            properties = (Map<String, Object>) yaml.load(in);

        } catch (final IOException | YAMLException e) {
            logger.error("Config file not exists", e);
            throw new ApplicationException("Config file not exists", e);
        }
        return properties;
    }

    /**
     * Merge two map of properties
     *
     * @param common   source map
     * @param specific extra properties
     * @return merge map
     */
    public static Map mergeProperties(final Map common, final Map specific) {
        for (final Object key : specific.keySet()) {
            if (specific.get(key) instanceof Map && common.get(key) instanceof Map) {
                common.put(key, mergeProperties((Map) common.get(key), (Map) specific.get(key)));
            } else {
                common.put(key, specific.get(key));
            }
        }
        return common;
    }

    /**
     * Replace environment variables in properties
     *
     * @param properties properties
     * @return replaced properties
     */
    public static Map<String, Object> replaceEnvVariables(final Map<String, Object> properties) {
        for (final Map.Entry property : properties.entrySet()) {
            if (property.getValue() instanceof String) {
                final String value = (String) property.getValue();
                getStringProperty(properties, property, value);
            } else if (property.getValue() instanceof Integer) {
                property.setValue(String.valueOf(property.getValue()));
            } else if (property.getValue() instanceof Map) {
                property.setValue(replaceEnvVariables((Map<String, Object>) property.getValue()));
            }
        }
        return properties;
    }

    private static void getStringProperty(final Map<String, Object> properties, final Map.Entry property, final String value) {
        if (value.startsWith("${")) {
            final String[] envVariable = value.replace("${", "").replace("}", "").split(":", 2);
            final String envValue = System.getenv(envVariable[0]);
            final String defaultValue = envVariable.length > 1 ? envVariable[1] : "";
            property.setValue(envValue != null ? envValue : defaultValue);
        } else if ("$UUID".equals(value)) {
            property.setValue(UUID.randomUUID().toString());
        } else if (value.indexOf('#') > -1) {
            String fieldValue = (String) property.getValue();
            final String subField = fieldValue.substring(fieldValue.indexOf('#') + 1,
                    fieldValue.indexOf('#', fieldValue.indexOf('#') + 1));

            fieldValue = properties.get(subField) != null
                    ? fieldValue.replaceAll("#.*#", (String) properties.get(subField)) : fieldValue;

            property.setValue(fieldValue);
        }
    }
}
