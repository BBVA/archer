package com.bbva.ddd.domain;

import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.builders.sql.QueryBuilder;
import org.apache.avro.specific.SpecificRecordBase;

/**
 *
 */
public interface Domain {

    /**
     * With this method we can add new data processor builders to the Domain which run when application start.
     * Imagine that you want a readable state from any stream:
     * <pre>
     *  DomainBuilder
     *      .create(config)
     *      <b>.addDataProcessorBuilder("tableName", new SimpleGlobalTableStateBuilder("streamName"))</b>
     *      .start();
     *
     *  StoreUtil.getStore("tableName");
     * </pre>
     *
     * @param name    The name of the DataProcessor. This name will be accessible from the DataflowProcessorContext context
     * @param builder A DataflowBuilder instance. It can be a custom DataflowBuilder or one of those defined in the framework
     * @return Domain
     */
    Domain addDataProcessorBuilder(final String name, final DataflowBuilder builder);

    /**
     * With this method you can create processors based on queries. At the moment it supports KSQL queries.
     * For example:
     * <pre>
     * {@code
     *  final Map<String, String> columns = new HashMap<>();
     *  columns.put("objectField", "STRUCT<anyFieldInObject VARCHAR>");
     *  columns.put("otherField", "VARCHAR");
     *
     *  final CreateStreamQueryBuilder fooStream =
     *      QueryBuilderFactory
     *          .createStream("fooStreamName")
     *          .columns(columns)
     *          .with(
     *              QueryBuilderFactory
     *                  .withProperties()
     *                  .kafkaTopic("sourceStream")
     *                  .valueFormat(WithPropertiesClauseBuilder.AVRO_FORMAT)
     *          );
     *
     *  final CreateStreamQueryBuilder fooFilteredStream =
     *      QueryBuilderFactory
     *          .createStream("fooFilteredStreamName")
     *          .with(
     *              QueryBuilderFactory
     *                  .withProperties()
     *                  .kafkaTopic(usersFilteredStreamName)
     *                  .valueFormat(WithPropertiesClauseBuilder.AVRO_FORMAT)
     *          )
     *          .asSelect(
     *              QueryBuilderFactory
     *                  .selectQuery()
     *                  .addQueryFields(Collections.singletonList("objectField->anyFieldInObject as anyField"))
     *                  .from(fooStream)
     *                  .where("objectField->anyFieldInObject LIKE '%bar%'")
     *          );
     *
     *  DomainBuilder
     *      .create(config)
     *      <b>.addDataProcessorBuilder(fooStream);</b>
     *      <b>.addDataProcessorBuilder(fooFilteredStream);</b>
     *      .addDataProcessorBuilder("foo-filtered",
     *                             new SimpleGlobalTableStateBuilder(fooFilteredStream.name()))
     *      .start();
     * }</pre>
     *
     * @param queryBuilder A extended QueryBuilder instance like CreateStreamQueryBuilder.
     * @return Domain
     */
    Domain addDataProcessorBuilder(final QueryBuilder queryBuilder);

    /**
     * It's a commodity method to add entity state builders. You determine a basename which is the prefix of the changelog
     * topic, for example foo_data_changelog, and the store name. With this processor we can have the current entity state
     * getting data from the changelog.
     * You can use this method like this:
     * <pre>{@code
     *  DomainBuilder
     *      .create(config)
     *      .<String, Foo>addEntityStateProcessor("foo", String.class)
     *      .start();
     * }</pre>
     * Refer to {@link com.bbva.dataprocessors.builders.dataflows.states.EntityStateBuilder#EntityStateBuilder(String, Class) EntityStateBuilder} to know
     * what it do.
     *
     * @param basename Basename to get source changelog and to name readable table.
     * @param keyClass Class type of the record key. It's used to serialize and deserialize key.
     * @param <K>      Type of the record key. It mustn't a SpecificRecord or GenericRecord instance.
     * @param <V>      Type of the record value. It must a SpecificRecordBase instance
     * @return Domain
     */
    <K, V extends SpecificRecordBase> Domain addEntityStateProcessor(final String basename, final Class<K> keyClass);

    /**
     * This processor index a field as key from value. Given this AVRO scheme:
     * <pre>
     *  {
     *      "type": "record",
     *      "name": "Foobar",
     *      "namespace": "com.bbva.avro",
     *      "doc": "Structure of foobar event values.",
     *      "fields": [{
     *          "name": "foo",
     *          "doc": "example field",
     *          "default": null,
     *          "type": [
     *              "null",
     *              {
     *                  "name": "Foo",
     *                  "namespace": "com.bbva.avro.foobar",
     *                  "doc": "Structure of foo",
     *                  "type": "record",
     *                  "fields": [{
     *                      "name": "bar",
     *                      "doc": "example field inside object foo",
     *                      "type": ["null", "string"],
     *                      "default": null
     *                  }]
     *              }
     *          ]
     *      }]
     *  }
     * </pre>
     * We can index bar in a readable store to do searches:
     * <pre>{@code
     *  DomainBuilder
     *      .create(config)
     *      .<String, Foobar, String>indexFieldStateProcessor("indexedFoobarByBar", "sourceStreamName", "foo->bar", String.class, String.class)
     *      .start();
     *
     *  final ReadableStore<String, Foobar> store = StoreUtil.getStore("indexedFoobarByBar");
     *  final Foobar foobar = store.findById("barValue");
     * }</pre>
     *
     * @param storeName        Name of the readable store where the processed data will be stored.
     * @param sourceStreamName Name of the source stream where raw data are.
     * @param fieldPath        Path to field which will be indexed. For example, the path for a field bar in object foo will be foo-&#62;bar
     * @param keyClass         Class of the source key
     * @param resultKeyClass   Class of the result key
     * @param <K>              Type of the record key. It mustn't a SpecificRecord or GenericRecord instance.
     * @param <V>              Type of the record value. It must a SpecificRecordBase instance.
     * @param <K1>             Type of the new record-key. It mustn't a SpecificRecord or GenericRecord instance.
     * @return Domain
     */
    <K, V extends SpecificRecordBase, K1> Domain indexFieldStateProcessor(
            final String storeName, final String sourceStreamName, final String fieldPath,
            final Class<K> keyClass, final Class<K1> resultKeyClass);

    /**
     * Processor to group values by a field. Given this AVRO scheme:
     * <pre>
     *   {
     *      "type": "record",
     *      "name": "Foobar",
     *      "namespace": "com.bbva.avro",
     *      "doc": "Structure of foobar event values.",
     *      "fields": [
     *          {
     *              "name": "foo",
     *              "doc": "example field",
     *              "default": null,
     *              "type": ["null", "string"],
     *          },
     *          {
     *              "name": "bar",
     *              "doc": "example field",
     *              "default": null,
     *              "type": ["null", "string"],
     *         }
     *      ]
     *  }
     * </pre>
     * We can group Foobar objects by foo field in a readable store to get a GenericRecord with a list field which is an
     * array of Foobar objects:
     * <pre>{@code
     *  DomainBuilder
     *      .create(config)
     *      .<String, Foobar, String>groupByFieldStateProcessor("groupedFoobarByBar", "sourceStreamName", "foo", String.class, Foobar.class)
     *      .start();
     *
     *  final ReadableStore<String, GenericRecord> store = StoreUtil.getStore("groupedFoobarByBar");
     *  final GenericRecord foobarRecord = store.findById("fooValue");
     *  final GenericRecordList foobarRecordList = new GenericRecordList<>(Foobar.class);
     *  final List<Foobar> foobarList = foobarRecordList.getList(foobarRecord);
     * }</pre>
     *
     * @param storeName        Name of the readable store where the processed data will be stored.
     * @param sourceStreamName Name of the source stream where raw data are.
     * @param fieldName        Field in value to group values and used as key.
     * @param keyClass         Class of both the source key and the field key
     * @param valueClass       Class of the value
     * @param <K>              Type of the record key. It mustn't a SpecificRecord or GenericRecord instance.
     * @param <V>              Type of the record value. It must a SpecificRecordBase instance.
     * @return Domain
     */
    <K, V extends SpecificRecordBase> Domain groupByFieldStateProcessor(
            final String storeName, final String sourceStreamName, final String fieldName,
            final Class<K> keyClass, final Class<V> valueClass);

    /**
     * Start the Domain (Handlers, Repositories and Processors)
     *
     * @return HelperDomain
     */
    HelperDomain start();
}
