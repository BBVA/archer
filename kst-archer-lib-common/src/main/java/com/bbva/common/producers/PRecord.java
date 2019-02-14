package com.bbva.common.producers;

import com.bbva.common.utils.RecordHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PRecord<K, V> extends ProducerRecord<K, V> {
    
    public PRecord(final String topic, final K key, final V value, final RecordHeaders headers) {
        super(topic, null, key, value, headers.getList());
    }

}
