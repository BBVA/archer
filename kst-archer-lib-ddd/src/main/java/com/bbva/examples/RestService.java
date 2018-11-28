package com.bbva.examples;

import com.bbva.ddd.ApplicationServices;
import com.bbva.examples.api.channels.ChannelsServices;
import com.bbva.examples.api.devices.DevicesServices;
import com.bbva.examples.api.users.FiscalDataServices;
import com.bbva.examples.api.users.SettingsServices;
import com.bbva.examples.api.wallets.WalletsServices;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

public class RestService {

    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private Server jettyServer;
    private FiscalDataServices fiscalDataServices;
    private SettingsServices settingsServices;
    private DevicesServices devicesServices;
    private WalletsServices walletsServices;
    private ChannelsServices channelsServices;

    RestService(ApplicationServices app) {
        fiscalDataServices = new FiscalDataServices(app);
        settingsServices = new SettingsServices(app);
        devicesServices = new DevicesServices(app);
        walletsServices = new WalletsServices(app);
        channelsServices = new ChannelsServices(app);
    }

    /**
     * Start an embedded Jetty Server
     *
     * @throws Exception
     */
    void start(Integer hostPort) throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(hostPort);
        jettyServer.setHandler(context);

        ResourceConfig rc = new ResourceConfig();
        rc.register(fiscalDataServices);
        rc.register(settingsServices);
        rc.register(devicesServices);
        rc.register(walletsServices);
        rc.register(channelsServices);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        jettyServer.start();
    }

    /**
     * Stop the Jetty Server
     *
     * @throws Exception
     */
    void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

}
