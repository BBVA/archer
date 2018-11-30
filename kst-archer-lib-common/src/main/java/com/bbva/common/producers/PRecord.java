package com.bbva.common.producers;

import com.bbva.common.utils.RecordHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PRecord<K, V> extends ProducerRecord<K, V> {

    public PRecord(String topic, K key, V value, RecordHeaders headers) {
        super(topic, null, key, value, headers.getList());
    }

}
