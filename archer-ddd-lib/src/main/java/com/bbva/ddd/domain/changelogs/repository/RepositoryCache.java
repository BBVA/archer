package com.bbva.ddd.domain.changelogs.repository;

import com.bbva.dataprocessors.exceptions.StoreNotFoundException;
import com.bbva.dataprocessors.states.States;
import com.bbva.dataprocessors.util.ObjectUtils;
import com.bbva.ddd.domain.changelogs.producers.ChangelogRecordMetadata;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.HashMap;
import java.util.Map;

/**
 * Manage the repositories caching in memory
 *
 * @param <V> Value class type
 */
public class RepositoryCache<V extends SpecificRecordBase> {
    private final Map<String, Record> records = new HashMap<>();
    private final Map<String, V> currentStates = new HashMap<>();

    /**
     * Update the state with specific key
     *
     * @param key            key to update
     * @param value          new values
     * @param recordMetadata metadata of the changelog
     */
    public void updateState(final String key, final V value, final ChangelogRecordMetadata recordMetadata) {
        final Record newRecord = new Record(key, value, recordMetadata);
        final V currentState;

        if (!currentStates.containsKey(key) || currentStates.get(key) == null || value == null) {
            currentState = value;
        } else {
            currentState = (V) ObjectUtils.merge(currentStates.get(key), value);
        }

        currentStates.put(key, currentState);
        records.put(key, newRecord);
    }

    /**
     * Get actual state of an entity
     *
     * @param baseName name of the state
     * @param key      key to get
     * @return state
     */
    public V getCurrentState(final String baseName, final String key) {
        V value;

        if (currentStates.containsKey(key)) {
            value = currentStates.get(key);
        } else {
            try {
                value = States.get().<String, V>getStore(baseName).findById(key);
            } catch (final StoreNotFoundException e) {
                value = null;
            }
        }
        return value;
    }

    /**
     * Get record by key
     *
     * @param key key to find
     * @return record found
     */
    public Record getRecord(final String key) {
        return records.get(key);
    }

    /**
     * Record properties wrapper
     */
    public class Record {
        private final String key;
        private final V value;
        private final ChangelogRecordMetadata metadata;

        Record(final String key, final V value, final ChangelogRecordMetadata metadata) {
            this.key = key;
            this.value = value;
            this.metadata = metadata;
        }

        public String key() {
            return key;
        }

        public V value() {
            return value;
        }

        public ChangelogRecordMetadata metadata() {
            return metadata;
        }
    }
}
