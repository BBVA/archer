
package com.bbva.dataprocessors.transformers.records;

import org.apache.kafka.streams.KeyValue;

public class KeyValueIListIterator implements org.apache.kafka.streams.state.KeyValueIterator {

    boolean isFirst = true;

    @Override
    public void close() {

    }

    @Override
    public Object peekNextKey() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return isFirst;
    }

    @Override
    public Object next() {
        isFirst = false;
        return new KeyValue<>("key", new GenericRecordImpl());
    }
}
