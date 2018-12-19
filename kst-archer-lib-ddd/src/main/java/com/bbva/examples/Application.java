package com.bbva.examples;

import com.bbva.avro.Devices;
import com.bbva.avro.Users;
import com.bbva.avro.users.FiscalData;
import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.GenericClass;
import com.bbva.dataprocessors.ReadableStore;
import com.bbva.dataprocessors.builders.dataflows.states.SimpleGlobalTableStateBuilder;
import com.bbva.dataprocessors.builders.sql.QueryBuilderFactory;
import com.bbva.dataprocessors.builders.sql.queries.CreateStreamQueryBuilder;
import com.bbva.dataprocessors.builders.sql.queries.WithPropertiesClauseBuilder;
import com.bbva.ddd.ApplicationServices;
import com.bbva.ddd.domain.Domain;
import com.bbva.examples.aggregates.*;
import com.bbva.examples.aggregates.user.FiscalDataAggregate;
import com.google.common.collect.Lists;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO can affect to main services @Config(file = "examples/config.yml", dataflow = true, ksql = true)
public class Application {

    public static final String EMAIL_TOPIC_SOURCE = FiscalDataAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    public static final String PUBLIC_UUID_TOPIC_SOURCE = FiscalDataAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    public static final String EMAIL_STORE_BASENAME = EMAIL_TOPIC_SOURCE + "_email";
    public static final String PUBLIC_UUID_STORE_BASENAME = EMAIL_TOPIC_SOURCE + "_email";
    public static final String TEST_QUERY_STORE_BASENAME = "query_devices";

    public static void main(final String[] args) {

        final ApplicationConfig applicationConfig = new AppConfiguration().init();

        try {
            final Map<String, String> usersColumns = new HashMap<>();
            usersColumns.put("fiscalData", "STRUCT<email VARCHAR>");
            final String usersStreamName = UserAggregate.baseName()
                    + ApplicationConfig.KsqlProperties.KSQL_STREAM_SUFFIX;
            final CreateStreamQueryBuilder usersStream = QueryBuilderFactory.createStream(usersStreamName)
                    .columns(usersColumns)
                    .with(QueryBuilderFactory.withProperties()
                            .kafkaTopic(applicationConfig.streams()
                                    .get(ApplicationConfig.StreamsProperties.APPLICATION_NAME) + "_"
                                    + UserAggregate.baseName())
                            .valueFormat(WithPropertiesClauseBuilder.AVRO_FORMAT));

            final String usersFilteredStreamName = "users_filtered"
                    + ApplicationConfig.KsqlProperties.KSQL_STREAM_SUFFIX;
            final CreateStreamQueryBuilder usersFilteredStream = QueryBuilderFactory
                    .createStream(usersFilteredStreamName)
                    .with(QueryBuilderFactory.withProperties().kafkaTopic(usersFilteredStreamName)
                            .valueFormat(WithPropertiesClauseBuilder.AVRO_FORMAT))
                    .asSelect(QueryBuilderFactory.selectQuery().addQueryFields(Arrays.asList("fiscalData->email"))
                            .from(QueryBuilderFactory.createFrom(usersStream))
                            .where("fiscalData->email LIKE '%bbva%'"));

            final Map<String, String> walletsColumns = new HashMap<>();
            walletsColumns.put("deviceUuid", "VARCHAR");
            walletsColumns.put("address", "VARCHAR");
            final String walletsStreamName = WalletsAggregate.baseName()
                    + ApplicationConfig.KsqlProperties.KSQL_STREAM_SUFFIX;
            final CreateStreamQueryBuilder walletsStream = QueryBuilderFactory.createStream(walletsStreamName)
                    .columns(walletsColumns)
                    .with(QueryBuilderFactory.withProperties()
                            .kafkaTopic(applicationConfig.streams()
                                    .get(ApplicationConfig.StreamsProperties.APPLICATION_NAME) + "_"
                                    + WalletsAggregate.baseName())
                            .valueFormat(WithPropertiesClauseBuilder.AVRO_FORMAT));

            final Map<String, String> devicesColumns = new HashMap<>();
            devicesColumns.put("uuid", "VARCHAR");
            devicesColumns.put("alias", "VARCHAR");
            devicesColumns.put("publicUuid", "VARCHAR");
            final String devicesStreamName = DeviceAggregate.baseName()
                    + ApplicationConfig.KsqlProperties.KSQL_STREAM_SUFFIX;
            final CreateStreamQueryBuilder devicesStream = QueryBuilderFactory.createStream(devicesStreamName)
                    .columns(devicesColumns)
                    .with(QueryBuilderFactory.withProperties()
                            .kafkaTopic(applicationConfig.streams()
                                    .get(ApplicationConfig.StreamsProperties.APPLICATION_NAME) + "_"
                                    + DeviceAggregate.baseName())
                            .valueFormat(WithPropertiesClauseBuilder.AVRO_FORMAT));

            final Map<String, String> channelsColumns = new HashMap<>();
            channelsColumns.put("uuid", "VARCHAR");
            channelsColumns.put("protocol", "VARCHAR");
            channelsColumns.put("deviceUuid", "VARCHAR");
            final String channelsStreamName = ChannelsAggregate.baseName()
                    + ApplicationConfig.KsqlProperties.KSQL_STREAM_SUFFIX;
            final CreateStreamQueryBuilder channelsStream = QueryBuilderFactory.createStream(channelsStreamName)
                    .columns(channelsColumns)
                    .with(QueryBuilderFactory.withProperties()
                            .kafkaTopic(applicationConfig.streams()
                                    .get(ApplicationConfig.StreamsProperties.APPLICATION_NAME) + "_"
                                    + ChannelsAggregate.baseName())
                            .valueFormat(WithPropertiesClauseBuilder.AVRO_FORMAT));

            final String devicesByAddressStreamName = "users_devices_wallets_by_address"
                    + ApplicationConfig.KsqlProperties.KSQL_STREAM_SUFFIX;
            final CreateStreamQueryBuilder devicesByAddressStream = QueryBuilderFactory
                    .createStream(devicesByAddressStreamName)
                    .with(QueryBuilderFactory.withProperties().kafkaTopic(devicesByAddressStreamName)
                            .valueFormat(WithPropertiesClauseBuilder.AVRO_FORMAT))
                    .asSelect(QueryBuilderFactory.selectQuery()
                            .addQueryFields(Arrays.asList("d.alias", "d.uuid as deviceUuid", "w.address"))
                            .from(QueryBuilderFactory.createFrom(devicesStream.name() + " d")
                                    .join(QueryBuilderFactory.createJoin("INNER", walletsStream.name() + " w",
                                            "d.uuid=w.deviceUuid", QueryBuilderFactory.createWithin("1 HOURS")))))
                    .partitionBy("address");

            final String channelByAddressStreamName = "channels_by_address"
                    + ApplicationConfig.KsqlProperties.KSQL_STREAM_SUFFIX;
            final CreateStreamQueryBuilder channelByAddressStream = QueryBuilderFactory
                    .createStream(channelByAddressStreamName)
                    .with(QueryBuilderFactory.withProperties().kafkaTopic(channelByAddressStreamName)
                            .valueFormat(WithPropertiesClauseBuilder.AVRO_FORMAT))
                    .asSelect(QueryBuilderFactory.selectQuery()
                            .addQueryFields(Arrays.asList("dba.address", "c.uuid as channelUuid", "dba.alias"))
                            .from(QueryBuilderFactory.createFrom(devicesByAddressStream.name() + " dba")
                                    .join(QueryBuilderFactory.createJoin("INNER", channelsStream.name() + " c",
                                            "dba.deviceUuid=c.deviceUuid",
                                            QueryBuilderFactory.createWithin("1 HOURS")))))
                    .partitionBy("address");

            final Domain domain = new Domain(new MainHandler(new RootAggregate()), applicationConfig);

            final ApplicationServices app = domain
                    .<String, FiscalData, String>indexFieldAsLocalState(EMAIL_STORE_BASENAME, EMAIL_TOPIC_SOURCE,
                            "email", new GenericClass<>(String.class), new GenericClass<>(String.class))
                    .<String, Devices, String>indexFieldAsLocalState(PUBLIC_UUID_STORE_BASENAME,
                            PUBLIC_UUID_TOPIC_SOURCE, "publicUuid", new GenericClass<>(String.class),
                            new GenericClass<>(String.class))
                    // .<String, Devices>addEntityAsLocalState("devices", new GenericClass<>(String.class))
                    // .addDataProcessorBuilder("drop_detection", new DropDetectionBuilder())
                    .addDataProcessorBuilder(QueryBuilderFactory.dropStream(devicesStream, true))
                    .addDataProcessorBuilder(QueryBuilderFactory.dropStream(walletsStream, true))
                    .addDataProcessorBuilder(QueryBuilderFactory.dropStream(channelsStream, true))
                    .addDataProcessorBuilder(QueryBuilderFactory.dropStream(devicesByAddressStream, true))
                    .addDataProcessorBuilder(QueryBuilderFactory.dropStream(channelByAddressStream, true))
                    .addDataProcessorBuilder(QueryBuilderFactory.dropStream(usersStream, true))
                    .addDataProcessorBuilder(QueryBuilderFactory.dropStream(usersFilteredStream, true))
                    .addDataProcessorBuilder(devicesStream).addDataProcessorBuilder(walletsStream)
                    .addDataProcessorBuilder(channelsStream).addDataProcessorBuilder(devicesByAddressStream)
                    .addDataProcessorBuilder(TEST_QUERY_STORE_BASENAME,
                            new SimpleGlobalTableStateBuilder(devicesByAddressStream.name()))
                    .addDataProcessorBuilder(channelByAddressStream)
                    .addDataProcessorBuilder(TEST_QUERY_STORE_BASENAME + "2",
                            new SimpleGlobalTableStateBuilder(channelByAddressStream.name()))
                    .addDataProcessorBuilder(usersStream).addDataProcessorBuilder(usersFilteredStream)
                    .addDataProcessorBuilder("email-filtered",
                            new SimpleGlobalTableStateBuilder(usersFilteredStream.name()))
                    .start();

            // At this point, local states are filled
            final ReadableStore<String, Users> store = ApplicationServices.getStore(UserAggregate.baseName());
            final long numEntries = store.approximateNumEntries();
            KeyValueIterator<String, Users> users = null;
            List<KeyValue<String, Users>> usersList = null;
            while (users == null) {
                usersList = Lists.newArrayList(store.findAll());
                if (usersList.size() >= numEntries) {
                    users = store.findAll();
                } else {
                    Thread.sleep(100);
                }
            }

            final RestService rest = new RestService(app);

            rest.start(applicationConfig.getInteger("host.port"));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    rest.stop();
                } catch (final Exception e) {
                    // ignored
                }
            }));

        } catch (final Exception e) {
            e.printStackTrace();
        }

    }
}
