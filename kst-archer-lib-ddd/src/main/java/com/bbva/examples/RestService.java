package com.bbva.examples;

import com.bbva.ddd.HelperDomain;
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

public class RestService {

    private Server jettyServer;
    private final FiscalDataServices fiscalDataServices;
    private final SettingsServices settingsServices;
    private final DevicesServices devicesServices;
    private final WalletsServices walletsServices;
    private final ChannelsServices channelsServices;

    RestService(final HelperDomain app) {
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
    void start(final Integer hostPort) throws Exception {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(hostPort);
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(fiscalDataServices);
        rc.register(settingsServices);
        rc.register(devicesServices);
        rc.register(walletsServices);
        rc.register(channelsServices);
        rc.register(JacksonFeature.class);

        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);
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
