package com.bbva.gateway.api;

import com.bbva.gateway.config.Config;
import com.bbva.gateway.config.Configuration;
import com.bbva.gateway.config.ServiceConfig;
import com.bbva.gateway.service.IAsyncGatewayService;
import kst.logging.Logger;
import kst.logging.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;

import static com.bbva.gateway.constants.ConfigConstants.*;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CallbackRest {

    private static final Logger logger = LoggerFactory.getLogger(CallbackRest.class);
    protected static Configuration config;
    private static IAsyncGatewayService gateway;

    @PostConstruct
    private static void init() throws IllegalAccessException, InstantiationException {
        final Config annotationConfig = Configuration.findConfigAnnotation();
        final List<Class> serviceClasses = Configuration.getServiceClasses(annotationConfig.servicesPackage());
        config = new Configuration().init(annotationConfig);
        for (final Class serviceClass : serviceClasses) {
            final ServiceConfig serviceConfig = (ServiceConfig) serviceClass.getAnnotation(ServiceConfig.class);

            final LinkedHashMap<String, Object> gatewayConfig = Configuration.getServiceConfig(serviceConfig.file());
            final String commandAction = (String) gatewayConfig.get("commandAction");
            final String baseName = (String) config.getCustom().get(GATEWAY_TOPIC);
            config.setGateway(gatewayConfig);
            if (commandAction != null && config.getGateway().get(GATEWAY_SYNC) != null && !(Boolean) config.getGateway().get(GATEWAY_SYNC) && config.getGateway().get(GATEWAY_CALLBACK) != null) {
                final IAsyncGatewayService service = (IAsyncGatewayService) serviceClass.newInstance();
                service.init(config, baseName);
                service.postInitActions();
                gateway = service;
                return;
            }
        }
    }

    @POST
    public static Response callback(final String request) {
        logger.debug("Callback receive");
        return Response.ok().build();
    }

}
