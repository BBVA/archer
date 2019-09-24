package com.bbva.dataprocessors.records;

import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.util.Collections;
import java.util.List;

/**
 * List of generci record
 *
 * @param <V> specific record type
 */
public class GenericRecordList<V extends SpecificRecord> {

    private static final Logger logger = LoggerFactory.getLogger(GenericRecordList.class);
    private static final String listField = "list";
    private final Schema listSchema;

    /**
     * Constructor
     *
     * @param itemClass Class of list items
     */
    public GenericRecordList(final Class<V> itemClass) {
        listSchema = generateSchema(itemClass);
    }

    private Schema generateSchema(final Class<V> itemClass) {
        Schema itemSchema = null;
        try {
            final V itemInstance = itemClass.newInstance();
            itemSchema = itemInstance.getSchema();

        } catch (final IllegalAccessException | InstantiationException e) {
            logger.error(e.getMessage(), e);
        }

        final Schema aggregatedList = Schema.createRecord("AggregatedList", "", "com.bbva.archer.avro.generics", false);
        aggregatedList.setFields(Collections.singletonList(
                new Schema.Field(
                        listField,
                        Schema.createArray(itemSchema),
                        "List of objects",
                        null)
        ));
        return aggregatedList;
    }

    /**
     * Get list of items
     *
     * @param record Record with list
     * @return list
     */
    public List<V> getList(final GenericRecord record) {
        return (List<V>) SpecificData.get().deepCopy(((GenericData.Array) record.get(listField)).getSchema(), record.get(listField));
    }

    /**
     * Get record from list of items
     *
     * @param list items
     * @return record with list
     */
    public GenericRecord getRecord(final List<V> list) {
        final GenericRecordBuilder aggregateBuilder = new GenericRecordBuilder(listSchema);
        aggregateBuilder.set(listField, list);

        return aggregateBuilder.build();
    }
}
