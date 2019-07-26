package com.bbva.dataprocessors.processors;

import com.bbva.dataprocessors.util.PowermockExtension;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class EntityProcessorTest {

    @DisplayName("Create and init EntityProcessor ok")
    @Test
    public void initPorcessorOk() {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);

        final EntityProcessor entityProcessor = new EntityProcessor("stateStoreName");
        entityProcessor.init(processorContext);
        entityProcessor.close();

        Assertions.assertNotNull(entityProcessor);
    }

    @DisplayName("Process object ok")
    @Test
    public void processWithNoOldValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn(null);

        final EntityProcessor entityProcessor = new EntityProcessor("stateStoreName");
        entityProcessor.init(processorContext);

        Exception ex = null;
        try {
            entityProcessor.process("key", "value");
        } catch (final Exception e) {
            ex = e;
        }

        Assertions.assertNull(ex);
    }

    @DisplayName("Process object ok")
    @Test
    public void processWithOldValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn("old");

        final EntityProcessor entityProcessor = new EntityProcessor("stateStoreName");
        entityProcessor.init(processorContext);

        Exception ex = null;
        try {
            entityProcessor.process("key", "value");
        } catch (final Exception e) {
            ex = e;
        }

        Assertions.assertNull(ex);
    }
}
