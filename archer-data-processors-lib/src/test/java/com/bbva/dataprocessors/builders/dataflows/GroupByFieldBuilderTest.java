package com.bbva.dataprocessors.builders.dataflows;

import com.bbva.common.config.AppConfig;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.TopicManager;
import com.bbva.dataprocessors.builders.dataflows.states.GroupByFieldStateBuilder;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({GroupByFieldStateBuilder.class, DataflowProcessorBuilder.class, TopicManager.class, StreamsBuilder.class})
public class GroupByFieldBuilderTest {

    @DisplayName("Create Dataflow processor and start")
    @Test
    public void createDataProcessorStart() throws Exception {

        final GroupByFieldStateBuilder groupByFieldStateBuilder = new GroupByFieldStateBuilder("snapshot", String.class, String.class, "fk");
        final DataflowProcessorBuilder dataflowProcessorBuilder = new DataflowProcessorBuilder(groupByFieldStateBuilder);

        final DataflowProcessorContext context = PowerMockito.mock(DataflowProcessorContext.class);
        final StreamsBuilder streamsBuilder = PowerMockito.mock(StreamsBuilder.class);

        PowerMockito.when(context, "schemaRegistryClient").thenReturn(PowerMockito.mock(CachedSchemaRegistryClient.class));
        PowerMockito.when(context, "streamsBuilder").thenReturn(streamsBuilder);
        PowerMockito.when(context, "configs").thenReturn(new AppConfig());
        PowerMockito.when(streamsBuilder, "build").thenReturn(new Topology());

        PowerMockito.whenNew(KafkaStreams.class).withAnyArguments().thenReturn(PowerMockito.mock(KafkaStreams.class));
        PowerMockito.mockStatic(TopicManager.class);

        dataflowProcessorBuilder.init(context);
        dataflowProcessorBuilder.start();

        Assertions.assertAll("dataProcessor",
                () -> Assertions.assertNotNull(dataflowProcessorBuilder)
        );
    }

}
