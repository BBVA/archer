package com.bbva.common.producers;

import com.bbva.common.producers.callback.ProducerCallback;
import com.bbva.common.producers.record.PRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * Producers interface to manage the production in the bus.
 * <pre>
 *  {@code
 *      final Producer producer = new DefaultProducer(configuration, Serdes.String().serializer(), Serdes.String().serializer(), true);
 *      final Future result = producer.send(new PRecord<>("test", "key", "value", new RecordHeaders()), producerCallback);
 *  }
 * </pre>
 */
public interface Producer {

    /**
     * Produce record in the event store
     *
     * @param record   message to produce
     * @param callback to manage asynchronous response of bus
     * @return production metadata
     */
    Future<RecordMetadata> send(final PRecord record, final ProducerCallback callback);

    /**
     * Method to to do actions after production. For example, commit transactions
     */
    default void end() {

    }
}