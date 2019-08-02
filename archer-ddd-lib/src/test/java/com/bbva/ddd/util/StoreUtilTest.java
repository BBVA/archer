package com.bbva.ddd.util;

import com.bbva.common.util.PowermockExtension;
import com.bbva.dataprocessors.ReadableStore;
import com.bbva.dataprocessors.States;
import com.bbva.dataprocessors.exceptions.StoreNotFoundException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({States.class})
public class StoreUtilTest {

    @DisplayName("get found store")
    @Test
    public void getStoreOk() throws Exception {
        final States states = PowerMockito.mock(States.class);
        PowerMockito.whenNew(States.class).withNoArguments().thenReturn(states);

        PowerMockito.when(states.getStore("test")).thenReturn(PowerMockito.mock(ReadableStore.class));

        final ReadableStore testStore = StoreUtil.getStore("test");

        Assertions.assertNotNull(testStore);
    }

    @DisplayName("get InvalidStateStoreException")
    @Test
    public void getStoreInvalidStateStoreExceptionOk() {

        Assertions.assertThrows(StoreNotFoundException.class, () -> {

            final States states = PowerMockito.mock(States.class);
            PowerMockito.whenNew(States.class).withNoArguments().thenReturn(states);

            PowerMockito.when(states.getStore("test")).thenThrow(InvalidStateStoreException.class).thenThrow(StoreNotFoundException.class);

            StoreUtil.getStore("test");

        });

    }

    @DisplayName("get not found store")
    @Test
    public void getStoreNotFound() {
        Assertions.assertThrows(StoreNotFoundException.class, () -> StoreUtil.getStore("test"));
    }

    @DisplayName("get not found store")
    @Test
    public void getStateStoreOk() throws Exception {
        final States states = PowerMockito.mock(States.class);
        PowerMockito.whenNew(States.class).withNoArguments().thenReturn(states);

        PowerMockito.when(states.getStoreState("test")).thenReturn(KafkaStreams.State.RUNNING);

        final boolean testStore = StoreUtil.checkStoreStatus("test");

        Assertions.assertTrue(testStore);
    }
}
