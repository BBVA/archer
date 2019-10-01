package com.bbva.dataprocessors.interactivequeries;

import com.bbva.common.exceptions.ApplicationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service to manage metadata
 */
public class MetadataService {

    private final KafkaStreams streams;

    /**
     * Constructor
     *
     * @param streams kafka streams
     */
    public MetadataService(final KafkaStreams streams) {
        this.streams = streams;
    }

    /**
     * Get the list of host store info
     *
     * @return list
     */
    public List<HostStoreInfo> streamsMetadata() {
        final Collection<StreamsMetadata> metadata = streams.allMetadata();
        return mapInstancesToHostStoreInfo(metadata);
    }

    /**
     * Get the list of host store info from a store
     *
     * @param store store name
     * @returnlist
     */
    public List<HostStoreInfo> streamsMetadataForStore(final String store) {
        final Collection<StreamsMetadata> metadata = streams.allMetadataForStore(store);
        return mapInstancesToHostStoreInfo(metadata);
    }

    /**
     * Get the host store info from a store and key
     *
     * @param store      store name
     * @param key        key
     * @param serializer key serializer
     * @param <K>        key class
     * @return information
     */
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
