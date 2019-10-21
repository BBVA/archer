package com.bbva.dataprocessors.interactivequeries;

import com.bbva.common.util.PowermockExtension;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

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
        final HostStoreInfo hostStoreInfoOtheCopy = new HostStoreInfo("host2", 1234, new HashSet<>());
        final Set storeNames = new HashSet<>();
        storeNames.add("store");
        final HostStoreInfo hostStoreInfoOtheCopy2 = new HostStoreInfo("host", 1234, storeNames);

        Assertions.assertAll("hostStoreInfo",
                () -> Assertions.assertNotNull(hostStoreInfo),
                () -> Assertions.assertNotNull(hostStoreInfo.getStoreNames()),
                () -> Assertions.assertNotNull(hostStoreInfo.toString()),
                () -> Assertions.assertTrue(hostStoreInfo.equals(hostStoreInfoCopy)),
                () -> Assertions.assertFalse(hostStoreInfo.equals(hostStoreInfoOtheCopy)),
                () -> Assertions.assertFalse(hostStoreInfo.equals(hostStoreInfoOtheCopy2)),
                () -> Assertions.assertTrue(hostStoreInfo.equals(hostStoreInfo)),
                () -> Assertions.assertFalse(hostStoreInfo.equals(null)),
                () -> Assertions.assertFalse("".equals(hostStoreInfo)),
                () -> Assertions.assertNotNull(hostStoreInfo.hashCode()),
                () -> Assertions.assertEquals(hostStoreInfo.getHost(), hostStoreInfoCopy.getHost()),
                () -> Assertions.assertEquals(hostStoreInfo.getPort(), hostStoreInfoCopy.getPort())
        );


    }

}
