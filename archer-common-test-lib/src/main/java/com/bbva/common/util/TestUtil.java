package com.bbva.common.util;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Utility for test
 */
public class TestUtil {

    /**
     * Get a random free port
     *
     * @param defaultPort default port
     * @return chosen port
     */
    public static int getFreePort(final int defaultPort) {
        try (final ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (final IOException e) {
            return defaultPort;
        }
    }

}
