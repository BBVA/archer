package com.bbva.archer.common.util;


import java.io.IOException;
import java.net.ServerSocket;


public class TestUtil {
    public static int getFreePort(final int defaultPort) {
        try (final ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (final IOException e) {
            return defaultPort;
        }
    }
}
