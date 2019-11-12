package com.bbva.common.consumers.managers;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Manager {

    void process(ConsumerRecords<String, SpecificRecordBase> records, boolean replay);

}
