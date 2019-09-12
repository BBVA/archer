package com.bbva.dataprocessors.transformers.records;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.HashMap;
import java.util.Map;

public class SpecificRecordImpl extends SpecificRecordBase {

    private static final long serialVersionUID = 2626763723720634172L;
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
        return values.get(i);
    }

    @Override
    public Schema getSchema() {
        return null;
    }

    public String getName() {
        return (String) values.get("name");
    }

    public String getUuid() {
        return "uuid";
    }
}
