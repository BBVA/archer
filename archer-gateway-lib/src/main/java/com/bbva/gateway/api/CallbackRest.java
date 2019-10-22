package com.bbva.gateway.api;

import com.bbva.gateway.config.ConfigBuilder;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.config.annotations.Config;
import com.bbva.gateway.config.annotations.ServiceConfig;
import com.bbva.gateway.service.IAsyncGatewayService;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Class to initialize the callback in async gateways
 */
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CallbackRest {

    private static final Logger logger = LoggerFactory.getLogger(CallbackRest.class);
    private static GatewayConfig config;

    /**
     * Post constructor actions
     */
    @PostConstruct
    public static void init() {
        final Config annotationConfig = ConfigBuilder.findConfigAnnotation();
        final List<Class> serviceClasses = ConfigBuilder.getServiceClasses(annotationConfig.servicesPackage());
        config = ConfigBuilder.create(annotationConfig);
        for (final Class serviceClass : serviceClasses) {
            final ServiceConfig serviceConfig = (ServiceConfig) serviceClass.getAnnotation(ServiceConfig.class);

            final LinkedHashMap<String, Object> gatewayConfig = ConfigBuilder.getServiceConfig(serviceConfig.file());
            final String commandAction = (String) gatewayConfig.get("commandAction");
            final String baseName = (String) config.custom(GatewayConfig.CustomProperties.GATEWAY_TOPIC);
            config.gateway().putAll(gatewayConfig);
            if (commandAction != null && config.gateway(GatewayConfig.GatewayProperties.GATEWAY_SYNC) != null && !(Boolean) config.gateway(GatewayConfig.GatewayProperties.GATEWAY_SYNC) && config.gateway(GatewayConfig.GatewayProperties.GATEWAY_CALLBACK) != null) {
                IAsyncGatewayService service = null;
                try {
                    service = (IAsyncGatewayService) serviceClass.newInstance();
                } catch (final InstantiationException | IllegalAccessException e) {
                    logger.error("Error instancing the service", e);
                }
                service.init(config, baseName);
                service.postInitActions();
                return;
            }
        }
    }

    /**
     * Endpoint enabled to manage callbacks
     *
     * @param request body
     * @return response ok
     */
    @POST
    public static Response callback(final String request) {
        logger.debug("Callback receive");
        return Response.ok().build();
    }

}
