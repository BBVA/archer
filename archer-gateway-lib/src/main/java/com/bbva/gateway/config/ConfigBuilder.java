package com.bbva.gateway.config;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.util.ConfigurationUtil;
import com.bbva.gateway.GatewayDomain;
import com.bbva.gateway.config.annotations.Config;
import com.bbva.gateway.config.annotations.ServiceConfig;
import org.reflections.Reflections;
import org.yaml.snakeyaml.Yaml;

import java.util.*;

/**
 * Start the general and service config of the gateways
 */
@com.bbva.common.config.annotations.Config()
public class ConfigBuilder {

    private GatewayConfig gatewayConfig;
    private static ConfigBuilder instance;
    private static final Yaml yaml = new Yaml();

    /**
     * Create and init application configuration
     *
     * @return configuration
     */
    public static GatewayConfig create(final Class classType) {
        instance = new ConfigBuilder();
        final Config configAnnotation = (Config) classType.getAnnotation(Config.class);
        return init(configAnnotation);
    }

    /**
     * Create and init application configuration
     *
     * @return configuration
     */
    public static GatewayConfig create() {
        instance = new ConfigBuilder();
        final Config configAnnotation = ConfigBuilder.findConfigAnnotation();
        return init(configAnnotation);
    }

    /**
     * Create and init application configuration
     *
     * @return configuration
     */
    public static GatewayConfig create(final Config extraConfig) {
        instance = new ConfigBuilder();
        return init(extraConfig);
    }


    /**
     * Get instance of AppConfiguration
     *
     * @return appConfiguration instance
     */
    public static AppConfig get() {
        return instance.gatewayConfig;
    }

    /**
     * Initialize gateway with extra config annotation
     *
     * @param extraConfig extra configuration
     * @return configuration instance
     */
    private static GatewayConfig init(final Config extraConfig) {
        final GatewayConfig gatewayConfig = new GatewayConfig();

        final Map<String, Object> config = getConfig(extraConfig);
        final LinkedHashMap<String, Object> application = (LinkedHashMap<String, Object>) config.get(GatewayConfig.GATEWAY_APPLICATION_PROPERTIES);

        final AppConfig appConfig = com.bbva.common.config.ConfigBuilder.create(ConfigBuilder.class.getAnnotation(com.bbva.common.config.annotations.Config.class));

        addConfigProperties(gatewayConfig.get(), application, GatewayConfig.GATEWAY_APP_PROPERTIES, appConfig.get());
        addConfigProperties(gatewayConfig.consumer(), application, GatewayConfig.GATEWAY_CONSUMER_PROPERTIES, appConfig.consumer());
        addConfigProperties(gatewayConfig.producer(), application, GatewayConfig.GATEWAY_PRODUCER_PROPERTIES, appConfig.producer());
        addConfigProperties(gatewayConfig.streams(), application, GatewayConfig.GATEWAY_STREAM_PROPERTIES, appConfig.streams());
        addConfigProperties(gatewayConfig.ksql(), application, GatewayConfig.GATEWAY_KSQL_PROPERTIES, appConfig.ksql());
        addConfigProperties(gatewayConfig.dataflow(), application, GatewayConfig.GATEWAY_DATAFLOW_PROPERTIES, appConfig.dataflow());

        gatewayConfig.custom().putAll((LinkedHashMap<String, Object>) config.get(GatewayConfig.GATEWAY_CUSTOM_PROPERTIES));
        gatewayConfig.gateway().putAll((LinkedHashMap<String, Object>) config.get(GatewayConfig.GATEWAY_GATEWAY_PROPERTIES));

        instance.gatewayConfig = gatewayConfig;
        return gatewayConfig;
    }

    private static void addConfigProperties(final Properties properties, final LinkedHashMap<String, Object> config,
                                            final String applicationProperties, final Properties originalProperties) {
        if (originalProperties != null) {
            properties.putAll(originalProperties);
        }
        if (config.get(applicationProperties) != null) {
            properties.putAll((Map<?, ?>) config.get(GatewayConfig.GATEWAY_COMMON_PROPERTIES));
            properties.putAll((Map<?, ?>) config.get(applicationProperties));
        }
    }

    private static Map<String, Object> getConfig(final Config extraConfig) {

        final ClassLoader classLoader = GatewayDomain.class.getClassLoader();
        Map<String, Object> properties = ConfigurationUtil.getConfigFromFile(yaml, classLoader, GatewayConfig.COMMON_CONFIG);
        if (extraConfig != null) {
            properties = ConfigurationUtil.mergeProperties(properties, ConfigurationUtil.getConfigFromFile(yaml, classLoader, extraConfig.file()));
        }

        properties = ConfigurationUtil.replaceEnvVariables(properties);
        return properties;
    }


    /**
     * Find gateway configuration annotation in all scaffolding
     *
     * @return config annotation
     */
    public static Config findConfigAnnotation() {
        final Reflections ref = new Reflections(GatewayDomain.class.getPackage().getName().split("\\.")[0]);
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
        final Reflections ref = new Reflections(servicesPackage != null && !"".equals(servicesPackage) ? servicesPackage : GatewayDomain.class.getPackage().getName().split("\\.")[0]);

        return new ArrayList<>(ref.getTypesAnnotatedWith(ServiceConfig.class));
    }

    /**
     * Get specific service configuration
     *
     * @param file file with config
     * @return map of config
     */
    public static Map<String, Object> getServiceConfig(final String file) {
        final ClassLoader classLoader = GatewayDomain.class.getClassLoader();
        Map<String, Object> properties = ConfigurationUtil.getConfigFromFile(yaml, classLoader, file);

        properties = ConfigurationUtil.replaceEnvVariables(properties);
        return properties;
    }
}
