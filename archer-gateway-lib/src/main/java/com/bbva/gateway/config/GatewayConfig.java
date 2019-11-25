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

    private final GatewayProperties gatewayProperties = new GatewayProperties();
    private final CustomProperties customProperties = new CustomProperties();
    private String servicesPackage;

    /**
     * Return stream properties
     *
     * @return property value
     */
    public Properties gateway() {
        return gatewayProperties.get();
    }

    /**
     * Return specific stream property
     *
     * @return properties
     */
    public Object gateway(final String property) {
        return gatewayProperties.get(property);
    }

    /**
     * Return stream properties
     *
     * @return property value
     */
    public Properties custom() {
        return customProperties.get();
    }

    /**
     * Return specific stream property
     *
     * @return properties
     */
    public Object custom(final String property) {
        return customProperties.get(property);
    }


    /**
     * Custom properties
     */
    public final class GatewayProperties extends PropertiesClass {
        public static final String GATEWAY_URI = "uri";
        public static final String GATEWAY_HTTP_HEADERS = "headers";
        public static final String GATEWAY_HTTP_METHOD = "method";
        public static final String GATEWAY_QUERY_PARAMS = "queryParams";

        public static final String GATEWAY_ATTEMPS = "attemps";
        public static final String GATEWAY_ATTEMP_SECONDS = "seconds";
        public static final String GATEWAY_RETRY = "retry";
        public static final String GATEWAY_RETRY_ENABLED = "enabled";

        public static final String GATEWAY_SYNC = "synchronous";
        public static final String GATEWAY_CALLBACK = "callback";
        public static final String GATEWAY_REST_PORT = "port";
        public static final String GATEWAY_REST_RESOURCE = "resource";
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
