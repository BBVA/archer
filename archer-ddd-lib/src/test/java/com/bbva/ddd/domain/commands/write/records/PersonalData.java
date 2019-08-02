/**
 * Autogenerated by Avro
 * <p>
 * DO NOT EDIT DIRECTLY
 */
package com.bbva.ddd.domain.commands.write.records;

import org.apache.avro.specific.SpecificData;

/**
 * Personal data of the user
 */
@org.apache.avro.specific.AvroGenerated
public class PersonalData extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    private static final long serialVersionUID = 1493172409439790256L;
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"PersonalData\",\"namespace\":\"com.bbva.ddd.domain.commands.write.records\",\"doc\":\"Personal data of the user\",\"fields\":[{\"name\":\"name\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"First name of the user\",\"default\":null},{\"name\":\"firstLastname\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"First family name of the user\",\"default\":null},{\"name\":\"secondLastname\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"Second family name of the user (not in all countries)\",\"default\":null},{\"name\":\"nationality\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"Nationality of the user\",\"default\":null},{\"name\":\"identityDocument\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"Identity document of the user\",\"default\":null},{\"name\":\"birthCountry\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"Country of birth of the user\",\"default\":null},{\"name\":\"birthday\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"Birth date of the user\",\"default\":null}]}");

    public static org.apache.avro.Schema getClassSchema() {
        return SCHEMA$;
    }

    /**
     * First name of the user
     */
    public PersonalData personalData;

    /**
     * Default constructor.  Note that this does not initialize fields
     * to their default values from the schema.  If that is desired then
     * one should use <code>newBuilder()</code>.
     */
    public PersonalData() {
    }

    /**
     * All-args constructor.
     *
     * @param personalData First name of the user
     */
    public PersonalData(final PersonalData personalData) {
        this.personalData = personalData;
    }

    @Override
    public org.apache.avro.Schema getSchema() {
        return SCHEMA$;
    }

    // Used by DatumWriter.  Applications should not call.
    @Override
    public Object get(final int field$) {
        switch (field$) {
            case 0:
                return personalData;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader.  Applications should not call.
    @Override
    public void put(final int field$, final Object value$) {
        switch (field$) {
            case 0:
                personalData = (PersonalData) value$;
                break;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    /**
     * Gets the value of the 'name' field.
     *
     * @return First name of the user
     */
    public PersonalData getPersonalData() {
        return personalData;
    }

    /**
     * Sets the value of the 'name' field.
     * First name of the user
     *
     * @param personalData the value to set.
     */
    public void setPersonalData(final PersonalData personalData) {
        this.personalData = personalData;
    }

    /**
     * Creates a new PersonalData RecordBuilder.
     *
     * @return A new PersonalData RecordBuilder
     */
    public static com.bbva.ddd.domain.commands.write.records.PersonalData.Builder newBuilder() {
        return new com.bbva.ddd.domain.commands.write.records.PersonalData.Builder();
    }

    /**
     * Creates a new PersonalData RecordBuilder by copying an existing Builder.
     *
     * @param other The existing builder to copy.
     * @return A new PersonalData RecordBuilder
     */
    public static com.bbva.ddd.domain.commands.write.records.PersonalData.Builder newBuilder(final com.bbva.ddd.domain.commands.write.records.PersonalData.Builder other) {
        return new com.bbva.ddd.domain.commands.write.records.PersonalData.Builder(other);
    }

    /**
     * Creates a new PersonalData RecordBuilder by copying an existing PersonalData instance.
     *
     * @param other The existing instance to copy.
     * @return A new PersonalData RecordBuilder
     */
    public static com.bbva.ddd.domain.commands.write.records.PersonalData.Builder newBuilder(final com.bbva.ddd.domain.commands.write.records.PersonalData other) {
        return new com.bbva.ddd.domain.commands.write.records.PersonalData.Builder(other);
    }

    /**
     * RecordBuilder for PersonalData instances.
     */
    public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<PersonalData>
            implements org.apache.avro.data.RecordBuilder<PersonalData> {

        /**
         * First name of the user
         */
        private PersonalData personalData;

        /**
         * Creates a new Builder
         */
        private Builder() {
            super(SCHEMA$);
        }

        /**
         * Creates a Builder by copying an existing Builder.
         *
         * @param other The existing Builder to copy.
         */
        private Builder(final com.bbva.ddd.domain.commands.write.records.PersonalData.Builder other) {
            super(other);
            if (isValidValue(fields()[0], other.personalData)) {
                personalData = data().deepCopy(fields()[0].schema(), other.personalData);
                fieldSetFlags()[0] = true;
            }
        }

        /**
         * Creates a Builder by copying an existing PersonalData instance
         *
         * @param other The existing instance to copy.
         */
        private Builder(final com.bbva.ddd.domain.commands.write.records.PersonalData other) {
            super(SCHEMA$);
            if (isValidValue(fields()[0], other.getPersonalData())) {
                personalData = data().deepCopy(fields()[0].schema(), other.getPersonalData());
                fieldSetFlags()[0] = true;
            }
        }

        /**
         * Gets the value of the 'name' field.
         * First name of the user
         *
         * @return The value.
         */
        public PersonalData getPersonalData() {
            return personalData;
        }

        /**
         * Sets the value of the 'name' field.
         * First name of the user
         *
         * @param value The value of 'name'.
         * @return This builder.
         */
        public com.bbva.ddd.domain.commands.write.records.PersonalData.Builder setPersonalData(final PersonalData value) {
            validate(fields()[0], value);
            personalData = value;
            fieldSetFlags()[0] = true;
            return this;
        }

        /**
         * Checks whether the 'name' field has been set.
         * First name of the user
         *
         * @return True if the 'name' field has been set, false otherwise.
         */
        public boolean hasPersonalData() {
            return fieldSetFlags()[0];
        }


        /**
         * Clears the value of the 'name' field.
         * First name of the user
         *
         * @return This builder.
         */
        public com.bbva.ddd.domain.commands.write.records.PersonalData.Builder clearPersonalData() {
            personalData = null;
            fieldSetFlags()[0] = false;
            return this;
        }

        @Override
        public PersonalData build() {
            try {
                final PersonalData record = new PersonalData();
                record.personalData = fieldSetFlags()[0] ? personalData : (PersonalData) defaultValue(fields()[0]);
                return record;
            } catch (final Exception e) {
                throw new org.apache.avro.AvroRuntimeException(e);
            }
        }
    }

    private static final org.apache.avro.io.DatumWriter
            WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);

    @Override
    public void writeExternal(final java.io.ObjectOutput out)
            throws java.io.IOException {
        WRITER$.write(this, SpecificData.getEncoder(out));
    }

    private static final org.apache.avro.io.DatumReader
            READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

    @Override
    public void readExternal(final java.io.ObjectInput in)
            throws java.io.IOException {
        READER$.read(this, SpecificData.getDecoder(in));
    }

}