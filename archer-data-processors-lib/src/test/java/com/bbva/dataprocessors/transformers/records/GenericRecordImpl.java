package com.bbva.dataprocessors.transformers.records;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

public class GenericRecordImpl implements GenericRecord {

    private final Map<String, Object> values = new HashMap<>();

    @Override
    public void put(final String fieldName, final Object value) {
        values.put(fieldName, value);
    }

    @Override
    public Object get(final String fieldName) {
        return values.get(fieldName);
    }

    @Override
    public void put(final int i, final Object o) {

    }

    @Override
    public Object get(final int i) {
        return "value";
    }

    @Override
    public Schema getSchema() {
        return null;
    }

    public String getName() {
        return (String) values.get("name");
    }
}
