package com.bbva.dataprocessors.states;

import com.bbva.common.util.PowermockExtension;
import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.exceptions.StoreNotFoundException;
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
public class StatesTest {

    @DisplayName("Create States class ok")
    @Test
    public void createStatesOk() {

        final States states = States.get();
        final States statesCopy = States.get();

        Assertions.assertAll("states",
                () -> Assertions.assertEquals(states, statesCopy)
        );
    }


    @DisplayName("Add state not running and get it")
    @Test
    public void addStateNotRunning() throws Exception {
        final ProcessorBuilder builder = PowerMockito.mock(ProcessorBuilder.class);
        final KafkaStreams stream = PowerMockito.mock(KafkaStreams.class);

        PowerMockito.when(builder, "streams").thenReturn(stream);
        PowerMockito.when(stream, "state").thenReturn(KafkaStreams.State.NOT_RUNNING);

        final States states = States.get();
        states.add("test", builder);
        final KafkaStreams.State state = states.getStoreState("test");


        Assertions.assertEquals(state.isRunning(), false);
    }

    @DisplayName("Add state running and get it")
    @Test
    public void addStateRunning() throws Exception {
        final ProcessorBuilder builder = PowerMockito.mock(ProcessorBuilder.class);
        final KafkaStreams stream = PowerMockito.mock(KafkaStreams.class);

        PowerMockito.when(builder, "streams").thenReturn(stream);
        PowerMockito.when(stream, "state").thenReturn(KafkaStreams.State.RUNNING);

        final States states = States.get();
        states.add("test", builder);
        final KafkaStreams.State state = states.getStoreState("test");

        Assertions.assertEquals(state.isRunning(), true);
    }

    @DisplayName("Get store not found")
    @Test
    public void getStoreNotFound() {
        Assertions.assertThrows(StoreNotFoundException.class, () -> {
            final States states = States.get();
            states.getStore("test");
        });
    }

    @DisplayName("Add state running and get it")
    @Test
    public void getStateStoreOk() throws Exception {
        final ProcessorBuilder builder = PowerMockito.mock(ProcessorBuilder.class);
        final KafkaStreams stream = PowerMockito.mock(KafkaStreams.class);

        PowerMockito.when(builder, "streams").thenReturn(stream);
        PowerMockito.when(stream, "state").thenReturn(KafkaStreams.State.RUNNING);

        final States states = States.get();
        states.add("test", builder);
        final ReadableStore store = states.getStore("test");
        final ReadableStore cachedStore = states.getStore("test");

        Assertions.assertEquals(store, cachedStore);
    }
}
