package com.bbva.common.config;

import com.bbva.common.config.annotations.Config;
import com.bbva.common.config.annotations.SecureConfig;
import com.bbva.common.config.util.ConfigurationUtil;
import org.reflections.Reflections;
import org.yaml.snakeyaml.Yaml;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Properties;

/**
 * Utility/builder class to populate system configurations
 */
public class ConfigBuilder {

    private AppConfig appConfig;
    private static ConfigBuilder instance;
    private static final Yaml yaml = new Yaml();

    /**
     * Create and init application configuration
     *
     * @return configuration
     */
    public static AppConfig create() {
        instance = new ConfigBuilder();
        return init();
    }

    /**
     * Create and init application configuration with secure config
     *
     * @param extraConfig secure config annotation
     * @return configuration
     */
    public static AppConfig create(final SecureConfig extraConfig) {
        instance = new ConfigBuilder();
        return init(extraConfig);
    }

    /**
     * Create and init application configuration with specific config annotation
     *
     * @param extraConfig config annotation
     * @return configuration
     */
    public static AppConfig create(final Config extraConfig) {
        instance = new ConfigBuilder();
        return init(extraConfig);
    }

    /**
     * Get instance of AppConfiguration
     *
     * @return appConfiguration instance
     */
    public static AppConfig get() {
        return instance.appConfig;
    }

    private static AppConfig init() {
        String mainPackage = Thread.currentThread().getStackTrace()[2].getClassName();
        mainPackage = removeLastPackage(mainPackage);

        final SecureConfig secureConfig = findConfigAnnotation(SecureConfig.class, mainPackage);
        if (secureConfig != null) {
            return init(secureConfig);
        }

        final Config extraConfig = findConfigAnnotation(Config.class, mainPackage);
        return init(extraConfig);
    }

    private static AppConfig init(final SecureConfig extraConfig) {
        final Map<String, Object> config = getConfig("common-secure-config.yml", extraConfig != null ? extraConfig.file() : null);

        return configure(config);
    }

    private static AppConfig init(final Config extraConfig) {
        final Map<String, Object> config = getConfig("common-config.yml", extraConfig != null ? extraConfig.file() : null);

        return configure(config);
    }

    private static <C extends Annotation> C findConfigAnnotation(final Class<C> annotationClass, final String packageName) {
        final Reflections ref = new Reflections(packageName);
        C configAnnotation = null;
        for (final Class<?> mainClass : ref.getTypesAnnotatedWith(annotationClass)) {
            configAnnotation = mainClass.getAnnotation(annotationClass);
        }

        if (configAnnotation != null) {
            return configAnnotation;
        } else if (packageName.indexOf('.') > 0) {
            return findConfigAnnotation(annotationClass, removeLastPackage(packageName));
        }
        return null;
    }

    private static AppConfig configure(final Map<String, Object> config) {
        instance.appConfig = new AppConfig();

        final Map<String, Object> appConfig = (Map<String, Object>) config.get("app");
        addConfigProperties(appConfig, instance.appConfig.get(), "application");
        addConfigProperties(appConfig, instance.appConfig.consumer(), "consumer");
        addConfigProperties(appConfig, instance.appConfig.producer(), "producer");
        addConfigProperties(appConfig, instance.appConfig.streams(), "streams");
        addConfigProperties(appConfig, instance.appConfig.ksql(), "ksql");
        addConfigProperties(appConfig, instance.appConfig.dataflow(), "dataflow");

        return instance.appConfig;
    }

    private static void addConfigProperties(final Map<String, Object> kafkaConfig, final Properties properties,
                                            final String applicationProperties) {
        if (kafkaConfig.get(applicationProperties) != null) {
            properties.putAll((Map<String, String>) kafkaConfig.get("common"));
            properties.putAll((Map<String, String>) kafkaConfig.get(applicationProperties));
        }
    }

    private static Map<String, Object> getConfig(final String commonFile, final String extraFile) {
        final ClassLoader classLoader = ConfigBuilder.class.getClassLoader();
        Map<String, Object> properties = ConfigurationUtil.getConfigFromFile(yaml, classLoader, commonFile);
        if (extraFile != null && !extraFile.isEmpty()) {
            properties = ConfigurationUtil.mergeProperties(properties, ConfigurationUtil.getConfigFromFile(yaml, classLoader, extraFile));
        }

        ConfigurationUtil.replaceEnvVariables(properties);
        return properties;
    }


    private static String removeLastPackage(final String mainPackage) {
        final int index = mainPackage.lastIndexOf('.');
        return mainPackage.substring(0, index);
    }
}
