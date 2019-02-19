package com.bbva.dataprocessors.interactivequeries;

import com.bbva.common.exceptions.ApplicationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class MetadataService {

    private final KafkaStreams streams;

    public MetadataService(final KafkaStreams streams) {
        this.streams = streams;
    }

    public List<HostStoreInfo> streamsMetadata() {
        final Collection<StreamsMetadata> metadata = streams.allMetadata();
        return mapInstancesToHostStoreInfo(metadata);
    }

    public List<HostStoreInfo> streamsMetadataForStore(final String store) {
        final Collection<StreamsMetadata> metadata = streams.allMetadataForStore(store);
        return mapInstancesToHostStoreInfo(metadata);
    }

    public <K> HostStoreInfo streamsMetadataForStoreAndKey(final String store, final K key, final Serializer<K> serializer) {
        final StreamsMetadata metadata = streams.metadataForKey(store, key, serializer);
        if (metadata == null) {
            throw new ApplicationException("Metadata not found for key " + key);
        }

        return new HostStoreInfo(metadata.host(), metadata.port(), metadata.stateStoreNames());
    }

    private static List<HostStoreInfo> mapInstancesToHostStoreInfo(final Collection<StreamsMetadata> metadatas) {
        return metadatas.stream()
                .map(metadata -> new HostStoreInfo(metadata.host(), metadata.port(), metadata.stateStoreNames()))
                .collect(Collectors.toList());
    }
}
