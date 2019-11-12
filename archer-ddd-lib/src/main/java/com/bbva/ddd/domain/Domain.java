package com.bbva.ddd.domain;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.dataprocessors.DataProcessor;
import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.EntityStateBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.GroupByFieldStateBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.UniqueFieldStateBuilder;
import com.bbva.dataprocessors.builders.sql.QueryBuilder;
import com.bbva.ddd.application.ApplicationHelper;
import com.bbva.ddd.domain.handlers.AutoConfiguredHandler;
import com.bbva.ddd.domain.handlers.Handler;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Initialize the domain with their data processors, repositories and configured handler.
 * For example if have class with autoconfigure annotations we could do:
 * <pre>
 * {@code
 *  Domain domain = new Domain.Builder(appConfig)
 *      .handler(new AnnotatedHandler(servicesPackage, config))
 *      .add("processor-name", new ChangelogKeyBuilder("processor-name", "internal-store"))
 *      .build();
 *
 *  domain.start()
 * }
 * </pre>
 */
public class Domain {

    private static final Logger logger = LoggerFactory.getLogger(Domain.class);
    
    private final AppConfig config;

    protected Domain(final AppConfig config) {

        this.config = config;
    }

    /**
     * Start the Domain (Handlers, Repositories and Processors)
     */
    public void start() {
        synchronized (this) {
            DataProcessor.get().start();
            logger.info("States have been started");

            Binder.get().start();

            ApplicationHelper.create(config);
        }
    }

    public static class Builder {
        private Handler handler;
        private final AppConfig config;
        private Domain domain;
        private static Builder instance;

        protected Builder(final AppConfig config) {
            domain = null;
            this.config = config;
            DataProcessor.create(config);
        }

        public static Builder create() {
            instance = new Builder(ConfigBuilder.get());
            return instance;
        }

        public static Builder create(final AppConfig configs) {
            instance = new Builder(configs);
            return instance;
        }

        public Domain.Builder handler(final Handler handler) {
            this.handler = handler;
            return this;
        }

        /**
         * With this method we can add new data processor builders to the Domain which run when application start.
         * Imagine that you want a readable state from any stream:
         * <pre>
         * {@code
         *  DomainBuilder
         *      .create(config)
         *      <b>.addDataProcessorBuilder("tableName", new SimpleGlobalTableStateBuilder("streamName"))</b>
         *      .start();
         *
         *  States.get().getStore("tableName");
         * }
         * </pre>
         *
         * @param name    The name of the DataProcessor. This name will be accessible from the DataflowProcessorContext context
         * @param builder A DataflowBuilder instance. It can be a custom DataflowBuilder or one of those defined in the framework
         * @return Domain builder
         */
        public Domain.Builder addDataProcessorBuilder(final String name, final DataflowBuilder builder) {
            DataProcessor.get().add(name, builder);
            return this;
        }

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
         * @return Domain builder
         */
        public Domain.Builder addDataProcessorBuilder(final QueryBuilder queryBuilder) {
            DataProcessor.get().add(queryBuilder);
            return this;
        }

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
         * @return Domain builder
         */
        public <K, V extends SpecificRecordBase> Domain.Builder addEntityStateProcessor(final String basename, final Class<K> keyClass) {
            final String snapshotTopicName = config.streams(AppConfig.StreamsProperties.APPLICATION_NAME) + "_" + basename;
            DataProcessor.get().add(basename, new EntityStateBuilder<K, V>(snapshotTopicName, keyClass));
            logger.info("Local state {} added", basename);
            return this;
        }

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
         *  final ReadableStore<String, Foobar> store = States.get().getStore("indexedFoobarByBar");
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
         * @return Domain builder
         */
        public <K, V extends SpecificRecordBase, K1> Domain.Builder indexFieldStateProcessor(
                final String storeName, final String sourceStreamName, final String fieldPath,
                final Class<K> keyClass, final Class<K1> resultKeyClass) {

            DataProcessor.get().add(storeName,
                    new UniqueFieldStateBuilder<K, V, K1>(sourceStreamName, fieldPath, keyClass, resultKeyClass));
            logger.info("Local state for index field {} added", fieldPath);
            return this;
        }

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
         *  final ReadableStore<String, GenericRecord> store = States.get().getStore("groupedFoobarByBar");
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
         * @return Domain builder
         */
        public <K, V extends SpecificRecordBase> Domain.Builder groupByFieldStateProcessor(
                final String storeName, final String sourceStreamName, final String fieldName
                , final Class<K> keyClass, final Class<V> valueClass) {

            DataProcessor.get().add(storeName,
                    new GroupByFieldStateBuilder<>(sourceStreamName, keyClass, valueClass, fieldName));
            logger.info("Local state map grouped by foreignKey field {} added", fieldName);
            return this;
        }

        public Domain build() {
            if (handler == null) {
                handler = new AutoConfiguredHandler();
            }

            Binder.create(config).build(handler);
            domain = new Domain(config);
            return domain;
        }

    }
}
