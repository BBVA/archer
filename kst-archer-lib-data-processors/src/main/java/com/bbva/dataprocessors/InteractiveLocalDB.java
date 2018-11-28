package com.bbva.dataprocessors;

import com.bbva.common.utils.interactivequeries.HostStoreInfo;
import com.bbva.common.utils.interactivequeries.MetadataService;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.glassfish.jersey.jackson.JacksonFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.util.HashMap;
import java.util.Map;

public class InteractiveLocalDB {

    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private final HostInfo hostInfo;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private final Map<String, ReadableStore> readableStores;

    public InteractiveLocalDB(final KafkaStreams streams, final HostInfo hostInfo) {
        this.streams = streams;
        this.metadataService = new MetadataService(streams);
        this.hostInfo = hostInfo;
        this.readableStores = new HashMap<>();
    }

    public <K, V> ReadableStore<K, V> getStore(String storeName) {
        ReadableStore<K, V> store;
        if (readableStores.containsKey(storeName)) {
            store = readableStores.get(storeName);
        } else {
            store = new ReadableStore<>(storeName);
            readableStores.put(storeName, new ReadableStore<>(storeName));
        }
        return store;
    }

    public class ReadableStore<K, V> {

        private final ReadOnlyKeyValueStore<K, V> store;

        ReadableStore(String storeName) {
            store = streams.store(storeName, QueryableStoreTypes.<K, V> keyValueStore());
        }

        private void check(String storeName, K key, Serde<K> keySerde) {
            final HostStoreInfo host = metadataService.streamsMetadataForStoreAndKey(storeName, key,
                    keySerde.serializer());

            if (!thisHost(host)) {
                // client.target(String.format("http://%s:%d/%s", host.getHost(), host.getPort(), path))
                // .request(MediaType.APPLICATION_JSON_TYPE)
                // .get(new GenericType<List<SongPlayCountBean>>() {
                // });
            }
        }

        private boolean thisHost(final HostStoreInfo host) {
            return host.getHost().equals(hostInfo.host()) && host.getPort() == hostInfo.port();
        }

        /**
         * @param key
         *            to look for in store
         * @return true if exists key, false otherwise
         */
        public boolean exists(K key) {
            return (findById(key) != null);
        }

        /**
         * @param key
         *            to look for in store
         * @return value V binded with the key param or null
         */
        public V findById(K key) {
            return store.get(key);
        }

        /**
         * @return Iterator with all key-values in the stores
         */
        public KeyValueIterator<K, V> findAll() {
            return store.all();
        }

        /**
         * @param from
         *            key
         * @param to
         *            key
         * @return Iterator with all values in range
         */
        public KeyValueIterator<K, V> range(K from, K to) {
            return store.range(from, to);
        }

        /**
         * @return approximate number of entries
         */
        public long approximateNumEntries() {
            return store.approximateNumEntries();
        }
    }
}
