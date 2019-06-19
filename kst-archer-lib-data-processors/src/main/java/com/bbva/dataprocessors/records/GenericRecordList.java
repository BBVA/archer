package com.bbva.dataprocessors.records;

import kst.logging.Logger;
import kst.logging.LoggerFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.util.Collections;
import java.util.List;

public class GenericRecordList<V extends SpecificRecord> {

    private static final Logger logger = LoggerFactory.getLogger(GenericRecordList.class);
    private static final String listField = "list";
    private final Schema listSchema;

    public GenericRecordList(final Class<V> itemClass) {
        this.listSchema = generateSchema(itemClass);
    }

    private Schema generateSchema(final Class<V> itemClass) {
        Schema itemSchema = null;
        try {
            final V itemInstance = itemClass.newInstance();
            itemSchema = itemInstance.getSchema();
            
        } catch (final IllegalAccessException | InstantiationException e) {
            logger.error(e.getMessage(), e);
        }

        final Schema AggregatedList = Schema.createRecord("AggregatedList", "", "com.bbva.archer.avro.generics", false);
        AggregatedList.setFields(Collections.singletonList(
                new Schema.Field(
                        listField,
                        Schema.createArray(itemSchema),
                        "List of objects",
                        null)
        ));
        return AggregatedList;
    }

    @SuppressWarnings("unchecked")
    public List<V> getList(final GenericRecord record) {
        return (List<V>) SpecificData.get().deepCopy(((GenericData.Array) record.get(listField)).getSchema(), record.get(listField));
    }

    public GenericRecord getRecord(final List<V> list) {
        final GenericRecordBuilder aggregateBuilder = new GenericRecordBuilder(listSchema);
        aggregateBuilder.set(listField, list);

        return aggregateBuilder.build();
    }
}
