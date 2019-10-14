package com.bbva.common.producers.record;

import com.bbva.common.utils.headers.RecordHeaders;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Producer record
 *
 * @param <K> Type of Record schema
 * @param <V> Type of Record
 */
public class PRecord extends ProducerRecord<String, SpecificRecordBase> {

    /**
     * Constructor
     *
     * @param topic   topic name
     * @param key     key
     * @param value   value
     * @param headers headers
     */
    public PRecord(final String topic, final String key, final SpecificRecordBase value, final RecordHeaders headers) {
        super(topic, null, key, value, headers.getList());
    }

}
