package com.ayoubom.kafka.utils;

import java.util.Collections;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;

import javax.ws.rs.NotFoundException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Looks up StreamsMetadata from KafkaStreams and converts the results
 * into Beans that can be JSON serialized via Jersey.
 */
public class MetadataService {

    private final KafkaStreams streams;

    public MetadataService(final KafkaStreams streams) {
        this.streams = streams;
    }

    /**
     * Get the metadata for all of the instances of this Kafka Streams application
     * @return List of {@link HostStoreInfo}
     */
    public List<HostStoreInfo> streamsMetadata() {
        // Get metadata for all of the instances of this Kafka Streams application
        final Collection<StreamsMetadata> metadata = streams.metadataForAllStreamsClients();
        return mapInstancesToHostStoreInfo(metadata);
    }

    /**
     * Get the metadata for all instances of this Kafka Streams application that currently
     * has the provided store.
     * @param store   The store to locate
     * @return  List of {@link HostStoreInfo}
     */
    public List<HostStoreInfo> streamsMetadataForStore(final  String store) {
        // Get metadata for all of the instances of this Kafka Streams application hosting the store
        final Collection<StreamsMetadata> metadata = streams.streamsMetadataForStore(store);
        return mapInstancesToHostStoreInfo(metadata);
    }

    /**
     * Find the metadata for the instance of this Kafka Streams Application that has the given
     * store and would have the given key if it exists.
     * @param store   Store to find
     * @param key     The key to find
     * @return {@link HostStoreInfo}
     */
    public <K> HostStoreInfo streamsMetadataForStoreAndKey(final String store,
                                                           final K key,
                                                           final Serializer<K> serializer) {
        // Get metadata for the instances of this Kafka Streams application hosting the store and
        // potentially the value for key
        final KeyQueryMetadata metadata = streams.queryMetadataForKey(store, key, serializer);
        if (metadata == null) {
            throw new NotFoundException();
        }

        return new HostStoreInfo(metadata.activeHost().host(),
                metadata.activeHost().port(),
                Collections.singleton(store));
    }

    private List<HostStoreInfo> mapInstancesToHostStoreInfo(
            final Collection<StreamsMetadata> metadatas) {
        return metadatas.stream().map(metadata -> new HostStoreInfo(metadata.host(),
                        metadata.port(),
                        metadata.stateStoreNames()))
                .collect(Collectors.toList());
    }

}
