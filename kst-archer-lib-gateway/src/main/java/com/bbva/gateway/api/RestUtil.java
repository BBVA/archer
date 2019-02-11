package com.bbva.gateway.api;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import kst.logging.Logger;
import kst.logging.LoggerFactory;

import javax.ws.rs.ext.RuntimeDelegate;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import static com.bbva.gateway.constants.ConfigConstants.GATEWAY_REST_PORT;
import static com.bbva.gateway.constants.ConfigConstants.GATEWAY_REST_RESOURCE;

public class RestUtil {

    private static final Logger logger = LoggerFactory.getLogger(RestUtil.class);
    public static final int DEFAULT_PORT = 8080;
    public static final String DEFAULT_RESOURCE = "/";
    public static final String LOCALHOST = "localhost";

    public static void startRestCallBack(final Map<String, Object> callbackConfig) throws IOException, InterruptedException {
        final Integer port = callbackConfig.get(GATEWAY_REST_PORT) != null ? Integer.valueOf(callbackConfig.get(GATEWAY_REST_PORT).toString())
                : DEFAULT_PORT;
        final String resource = callbackConfig.get(GATEWAY_REST_RESOURCE) != null
                ? (String) callbackConfig.get(GATEWAY_REST_RESOURCE) : DEFAULT_RESOURCE;
        startServer(port, resource);

        logger.info("Rest endpoint callback " + resource + " started started at port: " + port);

        Thread.currentThread().join();
    }


    private static void startServer(final int port, final String resource) throws IOException {
        final HttpServer server = HttpServer.create(new InetSocketAddress(LOCALHOST, port), 0);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(0)));

        final HttpHandler handler = RuntimeDelegate.getInstance().createEndpoint(new RestApplication(), HttpHandler.class);
        server.createContext(resource, handler);

        server.start();
    }
}
