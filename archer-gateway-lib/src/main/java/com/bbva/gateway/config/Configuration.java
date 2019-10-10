package com.bbva.gateway.config;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.config.util.ConfigurationUtil;
import com.bbva.gateway.Gateway;
import com.bbva.gateway.config.annotations.Config;
import com.bbva.gateway.config.annotations.ServiceConfig;
import org.reflections.Reflections;
import org.yaml.snakeyaml.Yaml;

import java.util.*;

import static com.bbva.gateway.constants.ConfigConstants.*;

/**
 * Start the general and service config of the gateways
 */
@com.bbva.common.config.annotations.Config()
public class Configuration {

    private AppConfig appConfig;
    private LinkedHashMap<String, Object> gateway;
    private LinkedHashMap<String, Object> custom;
    private static Configuration instance;

    /**
     * Get a configuration instance
     *
     * @return instance
     */
    public static Configuration get() {
        return instance;
    }

    /**
     * Get the configuration
     *
     * @return configuration
     */
    public AppConfig getAppConfig() {
        return appConfig;
    }

    /**
     * Set the configuration
     *
     * @param appConfig configuration
     */
    public void setAppConfig(final AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    /**
     * Set custom properties
     *
     * @param customConfig custom properties
     */
    public void setCustom(final LinkedHashMap<String, Object> customConfig) {
        custom = customConfig;
    }

    /**
     * Get custom properties
     *
     * @return properties
     */
    public Map<String, Object> getCustom() {
        return custom;
    }

    /**
     * Get gateway config
     *
     * @return properties
     */
    public Map<String, Object> getGateway() {
        return gateway;
    }

    /**
     * Set gateway config
     *
     * @param config properties
     */
    public void setGateway(final LinkedHashMap<String, Object> config) {
        gateway = config;
    }

    /**
     * Initialize gateway with extra config annotation
     *
     * @param extraConfig extra configuration
     * @return configuration instance
     */
    public Configuration init(final Config extraConfig) {

        final Map<String, Object> config = getConfig(extraConfig);
        custom = (LinkedHashMap<String, Object>) config.get(GATEWAY_CUSTOM_PROPERTIES);
        final LinkedHashMap<String, Object> application = (LinkedHashMap<String, Object>) config.get(GATEWAY_APPLICATION_PROPERTIES);
        gateway = (LinkedHashMap<String, Object>) config.get(GATEWAY_GATEWAY_PROPERTIES);

        appConfig = ConfigBuilder.create(Configuration.class.getAnnotation(com.bbva.common.config.annotations.Config.class));

        addConfigProperties(application, appConfig.get(), GATEWAY_APP_PROPERTIES);
        addConfigProperties(application, appConfig.consumer(), GATEWAY_CONSUMER_PROPERTIES);
        addConfigProperties(application, appConfig.producer(), GATEWAY_PRODUCER_PROPERTIES);
        addConfigProperties(application, appConfig.streams(), GATEWAY_STREAM_PROPERTIES);
        addConfigProperties(application, appConfig.ksql(), GATEWAY_KSQL_PROPERTIES);
        addConfigProperties(application, appConfig.dataflow(), GATEWAY_DATAFLOW_PROPERTIES);

        instance = this;
        return this;
    }

    private static void addConfigProperties(final LinkedHashMap<String, Object> config, final Properties properties,
                                            final String applicationProperties) {
        if (config.get(applicationProperties) != null) {
            properties.putAll((Map<?, ?>) config.get(GATEWAY_COMMON_PROPERTIES));
            properties.putAll((Map<?, ?>) config.get(applicationProperties));
        }
    }

    private static Map<String, Object> getConfig(final Config extraConfig) {
        final Yaml yaml = new Yaml();
        final ClassLoader classLoader = Gateway.class.getClassLoader();
        Map<String, Object> properties = ConfigurationUtil.getConfigFromFile(yaml, classLoader, COMMON_CONFIG);
        if (extraConfig != null) {
            properties = ConfigurationUtil.mergeProperties(properties, ConfigurationUtil.getConfigFromFile(yaml, classLoader, extraConfig.file()));
        }

        properties = ConfigurationUtil.replaceEnvVariables(properties);
        return properties;
    }

    /**
     * Find gateway configuration annotation in all scafolding
     *
     * @return config annotation
     */
    public static Config findConfigAnnotation() {
        final Reflections ref = new Reflections(Gateway.class.getPackage().getName().split("\\.")[0]);
        Config configAnnotation = null;
        for (final Class<?> mainClass : ref.getTypesAnnotatedWith(Config.class)) {
            configAnnotation = mainClass.getAnnotation(Config.class);
        }

        return configAnnotation;
    }

    /**
     * Get all annotated service classes
     *
     * @param servicesPackage main package to find
     * @return list of service classes
     */
    public static List<Class> getServiceClasses(final String servicesPackage) {
        final Reflections ref = new Reflections(!"".equals(servicesPackage) ? servicesPackage : Gateway.class.getPackage().getName().split("\\.")[0]);
        final List<Class> serviceClasses = new ArrayList<>();

        serviceClasses.addAll(ref.getTypesAnnotatedWith(ServiceConfig.class));

        return serviceClasses;
    }

    /**
     * Get specific service configuration
     *
     * @param file file with config
     * @return map of config
     */
    public static LinkedHashMap<String, Object> getServiceConfig(final String file) {
        final Yaml yaml = new Yaml();
        final ClassLoader classLoader = Gateway.class.getClassLoader();
        Map<String, Object> properties = ConfigurationUtil.getConfigFromFile(yaml, classLoader, file);

        properties = ConfigurationUtil.replaceEnvVariables(properties);
        return (LinkedHashMap<String, Object>) properties;
    }
}
