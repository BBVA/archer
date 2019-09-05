package com.bbva.dataprocessors;

import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({DefaultProducer.class, CachedProducer.class})
public class ReadableStoreTest {

    @DisplayName("Create readable store ok")
    @Test
    public void createStatesOk() throws Exception {
        final ReadOnlyKeyValueStore keyValueStore = PowerMockito.mock(ReadOnlyKeyValueStore.class);
        final KafkaStreams kafkaStreams = PowerMockito.mock(KafkaStreams.class);
        PowerMockito.when(kafkaStreams, "store", Mockito.any(String.class), Mockito.any(QueryableStoreType.class)).thenReturn(keyValueStore);

        final ReadableStore store = new ReadableStore("test_store", kafkaStreams);

        Assertions.assertAll("stores",
                () -> Assertions.assertNotNull(store)
        );
    }

    @DisplayName("Check exists store element")
    @Test
    public void checkExistsAndFindOk() throws Exception {
        final ReadOnlyKeyValueStore keyValueStore = PowerMockito.mock(ReadOnlyKeyValueStore.class);
        final KafkaStreams kafkaStreams = PowerMockito.mock(KafkaStreams.class);
        PowerMockito.when(kafkaStreams, "store", Mockito.any(String.class), Mockito.any(QueryableStoreType.class)).thenReturn(keyValueStore);

        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn("value");

        final ReadableStore store = new ReadableStore("test_store", kafkaStreams);
        final boolean exists = store.exists("key");
        final String value = (String) store.findById("key");

        Assertions.assertAll("stores",
                () -> Assertions.assertTrue(exists),
                () -> Assertions.assertEquals("value", value)
        );
    }

    @DisplayName("Check element that not exists")
    @Test
    public void checkNotExistsOk() throws Exception {
        final ReadOnlyKeyValueStore keyValueStore = PowerMockito.mock(ReadOnlyKeyValueStore.class);
        final KafkaStreams kafkaStreams = PowerMockito.mock(KafkaStreams.class);
        PowerMockito.when(kafkaStreams, "store", Mockito.any(String.class), Mockito.any(QueryableStoreType.class)).thenReturn(keyValueStore);

        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn(null);

        final ReadableStore store = new ReadableStore("test_store", kafkaStreams);

        Assertions.assertFalse(store.exists("key"));
    }

    @DisplayName("Find all, tange and check num entries")
    @Test
    public void findAllAndRangeOk() throws Exception {
        final ReadOnlyKeyValueStore keyValueStore = PowerMockito.mock(ReadOnlyKeyValueStore.class);
        final KafkaStreams kafkaStreams = PowerMockito.mock(KafkaStreams.class);
        final KeyValueIterator keyValueIterator = PowerMockito.mock(KeyValueIterator.class);

        PowerMockito.when(kafkaStreams, "store", Mockito.any(String.class), Mockito.any(QueryableStoreType.class)).thenReturn(keyValueStore);
        PowerMockito.when(keyValueStore, "all").thenReturn(keyValueIterator);
        PowerMockito.when(keyValueStore, "range", Mockito.any(), Mockito.any()).thenReturn(keyValueIterator);
        PowerMockito.when(keyValueStore, "approximateNumEntries").thenReturn(10L);

        final ReadableStore store = new ReadableStore("test_store", kafkaStreams);

        final KeyValueIterator allValues = store.findAll();
        final KeyValueIterator rangeValues = store.range(0, 10);
        final long approximateNumEntries = store.approximateNumEntries();

        Assertions.assertAll("stores",
                () -> Assertions.assertEquals(keyValueIterator, allValues),
                () -> Assertions.assertEquals(keyValueIterator, rangeValues),
                () -> Assertions.assertEquals(10L, approximateNumEntries)
        );
    }

}
