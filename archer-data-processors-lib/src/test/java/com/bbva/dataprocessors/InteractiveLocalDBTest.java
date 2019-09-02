package com.bbva.dataprocessors;

import com.bbva.common.util.PowermockExtension;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class InteractiveLocalDBTest {

    @DisplayName("Create interactive db ok")
    @Test
    public void createInteractiveDbOk() {
        final KafkaStreams stream = PowerMockito.mock(KafkaStreams.class);
        final InteractiveLocalDB localDB = new InteractiveLocalDB(stream);

        Assertions.assertAll("localDB",
                () -> Assertions.assertNotNull(localDB)
        );
    }

    @DisplayName("Create interactive db and readdable store ok")
    @Test
    public void createInteractiveDAndStoreOk() {
        final KafkaStreams stream = PowerMockito.mock(KafkaStreams.class);
        final InteractiveLocalDB localDB = new InteractiveLocalDB(stream);

        final ReadableStore store = localDB.getStore("test");
        final ReadableStore cachedStore = localDB.getStore("test");

        Assertions.assertAll("stores",
                () -> Assertions.assertNotEquals(store, cachedStore)
        );
    }
}
