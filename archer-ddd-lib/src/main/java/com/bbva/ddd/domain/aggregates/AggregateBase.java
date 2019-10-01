package com.bbva.ddd.domain.aggregates;

import com.bbva.common.producers.ProducerCallback;
import com.bbva.ddd.domain.aggregates.callbacks.ApplyRecordCallback;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Interface to define a aggregate methods
 *
 * @param <K> Key class
 * @param <V> Value class
 */
public interface AggregateBase<K, V extends SpecificRecordBase> {

    /**
     * Get current status of the entity in changelog
     *
     * @return the value
     */
    V getData();

    /**
     * Get entity identifier
     *
     * @return the key
     */
    K getId();

    /**
     * Apply new data in changelog
     *
     * @param method        Name of the method which apply new data
     * @param value         New value to apply
     * @param commandRecord Command record which has triggered the domain logic
     * @param callback      Callback executed when message is stored in changelog
     */
    void apply(String method, V value, CommandRecord commandRecord, ProducerCallback callback);

    /**
     * Internal method. Not for users
     *
     * @param apply a callback
     */
    void setApplyRecordCallback(ApplyRecordCallback apply);

    /**
     * Get class of the value
     *
     * @return Class of the value
     */
    Class<? extends SpecificRecord> getValueClass();
}
