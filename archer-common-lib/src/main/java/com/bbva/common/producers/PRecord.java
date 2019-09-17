package com.bbva.common.producers;

import com.bbva.common.utils.headers.RecordHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Producer record
 *
 * @param <K> Type of Record schema
 * @param <V> Type of Record
 */
public class PRecord<K, V> extends ProducerRecord<K, V> {

    /**
     * Constructor
     *
     * @param topic   topic name
     * @param key     key
     * @param value   value
     * @param headers headers
     */
    public PRecord(final String topic, final K key, final V value, final RecordHeaders headers) {
        super(topic, null, key, value, headers.getList());
    }

}
