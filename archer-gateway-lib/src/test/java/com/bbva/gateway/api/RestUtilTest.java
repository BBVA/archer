package com.bbva.gateway.api;

import com.bbva.common.exceptions.ApplicationException;
import com.bbva.gateway.util.PowermockExtension;
import com.sun.net.httpserver.HttpServer;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import javax.ws.rs.ext.RuntimeDelegate;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({RuntimeDelegate.class, HttpServer.class, RestUtil.class, Thread.class})
public class RestUtilTest {

    @DisplayName("Start RestUtil ok")
    @Test
    public void startRestOk() {
        Exception ex = null;
        final RestUtil restUtil = new RestUtil();
        try {
            PowerMockito.mockStatic(HttpServer.class);
            PowerMockito.mockStatic(RuntimeDelegate.class);
            PowerMockito.mockStatic(Thread.class);

            final HttpServer server = PowerMockito.mock(HttpServer.class);
            PowerMockito.when(HttpServer.create(Mockito.any(InetSocketAddress.class), Mockito.any(int.class))).thenReturn(server);
            PowerMockito.when(RuntimeDelegate.getInstance()).thenReturn(PowerMockito.mock(RuntimeDelegate.class));
            PowerMockito.doNothing().when(server, "start");

            RestUtil.startRestCallBack(new HashMap<>());
        } catch (final Exception e) {
            ex = e;
        }

        final Exception finalEx = ex;
        Assertions.assertAll("RestUtil",
                () -> Assertions.assertNull(finalEx),
                () -> Assertions.assertNotNull(restUtil)
        );
    }


    @DisplayName("Start RestUtil ko")
    @Test
    public void startRestKo() {
        Assertions.assertThrows(ApplicationException.class, () -> {

            PowerMockito.mockStatic(HttpServer.class);
            PowerMockito.mockStatic(RuntimeDelegate.class);
            PowerMockito.mockStatic(Thread.class);

            final HttpServer server = PowerMockito.mock(HttpServer.class);
            PowerMockito.when(HttpServer.create(Mockito.any(InetSocketAddress.class), Mockito.any(int.class))).thenThrow(IOException.class);
            PowerMockito.when(RuntimeDelegate.getInstance()).thenReturn(PowerMockito.mock(RuntimeDelegate.class));
            PowerMockito.doNothing().when(server, "start");

            RestUtil.startRestCallBack(new HashMap<>());

        });

    }
}
