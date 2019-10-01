package com.bbva.dataprocessors.interactivequeries;

import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.util.PowermockExtension;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class MetadataServiceTest {

    @DisplayName("Create service ok")
    @Test
    public void initPorcessorOk() {
        final KafkaStreams stream = Mockito.mock(KafkaStreams.class);

        final MetadataService metadataService = new MetadataService(stream);

        Assertions.assertNotNull(metadataService);
    }

    @DisplayName("Get not available metadata ok")
    @Test
    public void getAllMetadataNotAvailableOk() throws Exception {
        final KafkaStreams stream = Mockito.mock(KafkaStreams.class);

        final Collection<StreamsMetadata> metadata = new ArrayList<>();
        metadata.add(StreamsMetadata.NOT_AVAILABLE);

        PowerMockito.when(stream, "allMetadata").thenReturn(metadata);

        final MetadataService metadataService = new MetadataService(stream);
        final List<HostStoreInfo> storeInfo = metadataService.streamsMetadata();


        Assertions.assertAll("all metadata",
                () -> Assertions.assertNotNull(storeInfo),
                () -> Assertions.assertTrue(storeInfo.size() == 1),
                () -> Assertions.assertEquals("unavailable", storeInfo.get(0).getHost())
        );
    }

    @DisplayName("Get store metadata ok")
    @Test
    public void getStoreMetadataOk() throws Exception {
        final KafkaStreams stream = Mockito.mock(KafkaStreams.class);

        final Collection<StreamsMetadata> metadata = new ArrayList<>();
        metadata.add(new StreamsMetadata(new HostInfo("test_store", 1234), Collections.emptySet(), Collections.emptySet()));

        PowerMockito.when(stream, "allMetadataForStore", Mockito.anyString()).thenReturn(metadata);

        final MetadataService metadataService = new MetadataService(stream);
        final List<HostStoreInfo> storeInfo = metadataService.streamsMetadataForStore("test_store");

        Assertions.assertAll("store metadata",
                () -> Assertions.assertNotNull(storeInfo),
                () -> Assertions.assertTrue(storeInfo.size() == 1),
                () -> Assertions.assertEquals("test_store", storeInfo.get(0).getHost()),
                () -> Assertions.assertEquals(1234, storeInfo.get(0).getPort())
        );
    }

    @DisplayName("Not found metadata for store and key")
    @Test
    public void getStoreAndKeyMetadataNotFound() {
        final KafkaStreams stream = Mockito.mock(KafkaStreams.class);

        final MetadataService metadataService = new MetadataService(stream);

        final ApplicationException ex = null;
        Assertions.assertThrows(ApplicationException.class, () ->
                metadataService.streamsMetadataForStoreAndKey("test_store", "key", null)
        );

    }
}
