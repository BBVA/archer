package com.bbva.common.config;

import com.bbva.common.exceptions.ApplicationException;
import kst.logging.Logger;
import kst.logging.LoggerFactory;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class AppConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(AppConfiguration.class);

    private ApplicationConfig applicationConfig;
    private static AppConfiguration instance;

    public static AppConfiguration get() {
        return instance;
    }

    public ApplicationConfig getApplicationConfig() {
        return applicationConfig;
    }

    public ApplicationConfig init() {
        final Config extraConfig = getConfigAnnotation();
        return init(extraConfig);
    }

    public ApplicationConfig init(final Config extraConfig) {
        final Map<String, Object> config = getConfig(extraConfig);

        applicationConfig = new ApplicationConfig();

        final Map<String, Object> appConfig = (Map<String, Object>) config.get("app");
        addConfigProperties(appConfig, applicationConfig.get(), "application");
        addConfigProperties(appConfig, applicationConfig.consumer().get(), "consumer");
        addConfigProperties(appConfig, applicationConfig.producer().get(), "producer");
        addConfigProperties(appConfig, applicationConfig.streams().get(), "streams");
        addConfigProperties(appConfig, applicationConfig.ksql().get(), "ksql");
        addConfigProperties(appConfig, applicationConfig.dataflow().get(), "dataflow");

        instance = this;
        return applicationConfig;
    }

    private static void addConfigProperties(final Map<String, Object> kafkaConfig, final Properties properties,
                                            final String applicationProperties) {
        if (kafkaConfig.get(applicationProperties) != null) {
            properties.putAll((Map<String, String>) kafkaConfig.get("common"));
            properties.putAll((Map<String, String>) kafkaConfig.get(applicationProperties));
        }
    }

    private static Map<String, Object> getConfig(final Config extraConfig) {
        final Yaml yaml = new Yaml();
        final ClassLoader classLoader = AppConfiguration.class.getClassLoader();
        Map<String, Object> properties = getConfigFromFile(yaml, classLoader, "common-config.yml");
        if (extraConfig != null && !extraConfig.file().isEmpty()) {
            properties = mergeProperties(properties, getConfigFromFile(yaml, classLoader, extraConfig.file()));
        }

        replaceEnvVariables(properties);
        return properties;
    }

    public static Map<String, Object> getConfigFromFile(final Yaml yaml, final ClassLoader classLoader,
                                                        final String filename) {
        final Map<String, Object> properties;
        try (final InputStream in = classLoader.getResourceAsStream(filename)) {
            properties = (Map<String, Object>) yaml.load(in);

        } catch (final IOException e) {
            logger.error("Config file not exists", e);
            throw new ApplicationException("Config file not exists", e);
        }
        return properties;
    }

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

    public static void getStringProperty(final Map<String, Object> properties, final Map.Entry property, final String value) {
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

    public static Config getConfigAnnotation() {
        final Reflections ref = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(AppConfiguration.class.getPackage().getName().split("\\.")[0],
                        ClasspathHelper.contextClassLoader(), ClasspathHelper.staticClassLoader()))
                .filterInputsBy(new FilterBuilder().include(".+\\.class")));
        Config configAnnotation = null;
        for (final Class<?> mainClass : ref.getTypesAnnotatedWith(Config.class)) {
            configAnnotation = mainClass.getAnnotation(Config.class);
        }

        return configAnnotation;
    }
}
