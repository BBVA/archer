package com.bbva.common.producers.records;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordImpl implements GenericRecord {

    /**
     * Constructor
     */
    public GenericRecordImpl() {
        super();
    }

    @Override
    public void put(final String s, final Object o) {
        //Do nothing
    }

    @Override
    public Object get(final String s) {
        return null;
    }

    @Override
    public void put(final int i, final Object o) {
        //Do nothing
    }

    @Override
    public Object get(final int i) {
        return null;
    }

    @Override
    public Schema getSchema() {
        return null;
    }
}
