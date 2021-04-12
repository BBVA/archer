package com.bbva.gateway.config;

import com.bbva.common.config.AppConfig;

import java.util.Properties;

/**
 * Start the general and service config of the gateways
 */
@com.bbva.common.config.annotations.Config()
public class GatewayConfig extends AppConfig {

    public static final String COMMON_CONFIG = "common-config.yml";
    public static final String GATEWAY_COMMON_PROPERTIES = "common";
    public static final String GATEWAY_APP_PROPERTIES = "applicationProperties";
    public static final String GATEWAY_CUSTOM_PROPERTIES = "custom";
    public static final String GATEWAY_APPLICATION_PROPERTIES = "app";
    public static final String GATEWAY_GATEWAY_PROPERTIES = "gateway";
    public static final String GATEWAY_CONSUMER_PROPERTIES = "consumerProperties";
    public static final String GATEWAY_PRODUCER_PROPERTIES = "producerProperties";
    public static final String GATEWAY_STREAM_PROPERTIES = "streamsProperties";
    public static final String GATEWAY_KSQL_PROPERTIES = "ksql";
    public static final String GATEWAY_DATAFLOW_PROPERTIES = "dataflow";

    public static final String GATEWAY_COMMAND_ACTION = "commandAction";
    public static final String GATEWAY_EVENT_NAME = "event";

    public enum SourceTypes {
        COMMAND("command"),
        EVENT("event"),
        CHANGELOG("changelog");

        private final String name;

        SourceTypes(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private final GatewayProperties gatewayProperties = new GatewayProperties();
    private final CustomProperties customProperties = new CustomProperties();
    private String servicesPackage;

    /**
     * Return gateway properties
     *
     * @return property value
     */
    public Properties gateway() {
        return gatewayProperties.get();
    }

    /**
     * Return specific gateway property
     *
     * @return properties
     */
    public Object gateway(final String property) {
        return gatewayProperties.get(property);
    }

    /**
     * Return custom properties
     *
     * @return property value
     */
    public Properties custom() {
        return customProperties.get();
    }

    /**
     * Return specific custom property
     *
     * @return properties
     */
    public Object custom(final String property) {
        return customProperties.get(property);
    }


    /**
     * Gateway properties
     */
    public final class GatewayProperties extends PropertiesClass {
        public static final String GATEWAY_PROTOCOL = "protocol";
        public static final String GATEWAY_URI = "uri";
        public static final String GATEWAY_HTTP_METHOD = "method";

        public static final String GATEWAY_RETRY = "retry";
        public static final String GATEWAY_RETRY_ENABLED = "retry.enabled";
        public static final String GATEWAY_RETRY_ATTEMPTS = "retry.attempts";
        public static final String GATEWAY_RETRY_ATTEMPT_SECONDS = "retry.seconds";

        public static final String GATEWAY_SOURCE = "source";
        public static final String GATEWAY_SOURCE_NAME = "source.name";
        public static final String GATEWAY_SOURCE_TYPE = "source.type";
        public static final String GATEWAY_SOURCE_ACTION = "source.action";

        public static final String GATEWAY_ASYNC_CALLBACK = "async.callback";
        public static final String GATEWAY_ASYNC_METHOD = "async.method";
    }

    /**
     * Custom properties
     */
    public final class CustomProperties extends PropertiesClass {
        public static final String GATEWAY_TOPIC = "baseName";

    }

    public String getServicesPackage() {
        return servicesPackage;
    }

    public void setServicesPackage(final String servicesPackage) {
        this.servicesPackage = servicesPackage;
    }
}
