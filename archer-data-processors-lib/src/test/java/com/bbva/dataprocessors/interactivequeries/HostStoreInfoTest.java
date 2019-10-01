package com.bbva.dataprocessors.interactivequeries;

import com.bbva.common.util.PowermockExtension;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

import java.util.HashSet;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class HostStoreInfoTest {

    @DisplayName("Check all methods of host store bean")
    @Test
    public void checkHostoStoreMethodsOk() {
        final HostStoreInfo hostStoreInfo = new HostStoreInfo();
        hostStoreInfo.setHost("host");
        hostStoreInfo.setPort(1234);
        hostStoreInfo.setStoreNames(new HashSet<>());
        final HostStoreInfo hostStoreInfoCopy = new HostStoreInfo("host", 1234, new HashSet<>());

        Assertions.assertAll("hostStoreInfo",
                () -> Assertions.assertNotNull(hostStoreInfo),
                () -> Assertions.assertNotNull(hostStoreInfo.getStoreNames()),
                () -> Assertions.assertNotNull(hostStoreInfo.toString()),
                () -> Assertions.assertTrue(hostStoreInfo.equals(hostStoreInfoCopy)),
                () -> Assertions.assertTrue(hostStoreInfo.equals(hostStoreInfo)),
                () -> Assertions.assertFalse(hostStoreInfo.equals(null)),
                () -> Assertions.assertFalse(hostStoreInfo.equals("")),
                () -> Assertions.assertNotNull(hostStoreInfo.hashCode()),
                () -> Assertions.assertEquals(hostStoreInfo.getHost(), hostStoreInfoCopy.getHost()),
                () -> Assertions.assertEquals(hostStoreInfo.getPort(), hostStoreInfoCopy.getPort())
        );


    }

}
