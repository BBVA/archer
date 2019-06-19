package com.bbva.example.util;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.PRecord;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.common.utils.headers.types.EventHeaderType;
import com.bbva.gateway.config.Configuration;
import org.apache.avro.specific.SpecificRecord;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.bbva.gateway.constants.ConfigConstants.GATEWAY_TOPIC;

public class ExampleUtil<T extends SpecificRecord> {

    private static CachedProducer producer;

    public void generateEvents(final Configuration config, final T eventBody, final int replyTime, final List<String> commandActions) {
        producer = new CachedProducer(config.getApplicationConfig());
        final String topicName = (String) config.getCustom().get(GATEWAY_TOPIC);

        final ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.submit((Runnable) () -> {
            final String replayKey = "replayEvent" + new Date().getTime();
            final String replayCommand = commandActions.get((int) (new Date().getTime() % commandActions.size()));
            generateEvent(eventBody, topicName, replayKey, 0, false, replayCommand);

            while (true) {
                final String command = commandActions.get((int) (new Date().getTime() % commandActions.size()));
                final String key = "prueba" + new Date().getTime();
                generateEvent(eventBody, topicName, key, replyTime, false, command);

                if (new Date().getTime() % 50 == 0) {
                    generateEvent(eventBody, topicName, replayKey, replyTime, true, replayCommand);
                }
            }
        });
    }

    private void generateEvent(final T command, final String topicName, final String key, final int replyTime, final Boolean replay, final String commandAction) {
        producer.add(new PRecord<>(commandAction.equals("sns") ? "notifications" + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX : topicName + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX, replay ? UUID.randomUUID().toString() : key, command,
                generateHeaders(key, replay, commandAction)), (o, e) -> handlePutRecord(o, e));
        try {
            Thread.sleep(replyTime);
        } catch (final InterruptedException e) { //NOSONAR
            e.printStackTrace();
        }
    }

    private static RecordHeaders generateHeaders(final String key, final Boolean replay, final String commandAction) {

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue(commandAction));
        recordHeaders.add(EventHeaderType.REFERENCE_ID_KEY, new ByteArrayValue(key));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(replay));

        return recordHeaders;
    }

    private static void handlePutRecord(final Object o, final Exception e) {
    }
}
