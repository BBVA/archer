package com.bbva.common.producers;

import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.common.producers.record.PRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * Producers interface.
 * <pre>
 *  {@code
 *      final Producer producer = new DefaultProducer(configuration, Serdes.String().serializer(), Serdes.String().serializer(), true);
 *      final Future result = producer.save(new PRecord<>("test", "key", "value", new RecordHeaders()), producerCallback);
 *  }
 * </pre>
 *
 * @param <K> Type of Record schema
 * @param <V> Type of Record
 */
public interface Producer<K, V> {

    default void init() {

    }

    Future<RecordMetadata> send(final PRecord<K, V> record, final ProducerCallback callback);

    default void end() {

    }
}
