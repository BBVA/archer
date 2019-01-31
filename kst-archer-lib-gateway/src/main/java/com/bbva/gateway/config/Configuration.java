package com.bbva.gateway.config;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.gateway.Gateway;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.yaml.snakeyaml.Yaml;

import java.util.*;

import static com.bbva.gateway.constants.ConfigConstants.*;

public class Configuration {

    private ApplicationConfig applicationConfig;
    private LinkedHashMap<String, Object> gateway;
    private LinkedHashMap<String, Object> custom;
    private LinkedHashMap<String, Object> application;
    private static Configuration instance;

    public static Configuration get() {
        return instance;
    }

    public ApplicationConfig getApplicationConfig() {
        return applicationConfig;
    }

    public void setApplicationConfig(final ApplicationConfig appConfig) {
        applicationConfig = appConfig;
    }

    public void setCustom(final LinkedHashMap<String, Object> customConfig) {
        custom = customConfig;
    }

    public Map<String, Object> getCustom() {
        return custom;
    }

    public Map<String, Object> getGateway() {
        return gateway;
    }

    public void setGateway(final LinkedHashMap<String, Object> config) {
        gateway = config;
    }

    public Configuration init(final Config extraConfig) {
        final Map<String, Object> config = getConfig(extraConfig);
        custom = (LinkedHashMap<String, Object>) config.get(GATEWAY_CUSTOM_PROPERTIES);
        application = (LinkedHashMap<String, Object>) config.get(GATEWAY_APPLICATION_PROPERTIES);
        gateway = (LinkedHashMap<String, Object>) config.get(GATEWAY_GATEWAY_PROPERTIES);

        applicationConfig = new ApplicationConfig();

        addConfigProperties(application, applicationConfig.get(), GATEWAY_APP_PROPERTIES);
        addConfigProperties(application, applicationConfig.consumer().get(), GATEWAY_CONSUMER_PROPERTIES);
        addConfigProperties(application, applicationConfig.producer().get(), GATEWAY_PRODUCER_PROPERTIES);
        addConfigProperties(application, applicationConfig.streams().get(), GATEWAY_STREAM_PROPERTIES);

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
        Map<String, Object> properties = AppConfiguration.getConfigFromFile(yaml, classLoader, COMMON_CONFIG);
        if (extraConfig != null) {
            properties = AppConfiguration.mergeProperties(properties, AppConfiguration.getConfigFromFile(yaml, classLoader, extraConfig.file()));
        }

        properties = AppConfiguration.replaceEnvVariables(properties);
        return properties;
    }

    public static Config findConfigAnnotation() {
        final Reflections ref = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(Gateway.class.getPackage().getName().split("\\.")[0],
                        ClasspathHelper.contextClassLoader(), ClasspathHelper.staticClassLoader()))
                .filterInputsBy(new FilterBuilder().include(".+\\.class")));
        Config configAnnotation = null;
        for (final Class<?> mainClass : ref.getTypesAnnotatedWith(Config.class)) {
            configAnnotation = mainClass.getAnnotation(Config.class);
        }

        return configAnnotation;
    }


    public static List<Class> getServiceClasses(final String servicesPackage) {
        final Reflections ref = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(!servicesPackage.equals("") ? servicesPackage : Gateway.class.getPackage().getName().split("\\.")[0],
                        ClasspathHelper.contextClassLoader(), ClasspathHelper.staticClassLoader()))
                .filterInputsBy(new FilterBuilder().include(".+\\.class")));
        final List<Class> serviceClasses = new ArrayList<>();
        for (final Class<?> mainClass : ref.getTypesAnnotatedWith(ServiceConfig.class)) {
            serviceClasses.add(mainClass);
        }

        return serviceClasses;
    }

    public static LinkedHashMap<String, Object> getServiceConfig(final String file) {
        final Yaml yaml = new Yaml();
        final ClassLoader classLoader = Gateway.class.getClassLoader();
        Map<String, Object> properties = AppConfiguration.getConfigFromFile(yaml, classLoader, file);

        properties = AppConfiguration.replaceEnvVariables(properties);
        return (LinkedHashMap<String, Object>) properties;
    }
}
