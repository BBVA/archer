package com.bbva.common.utils;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.util.PowermockExtension;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest(TopicManager.class)
public class TopicManagerTest {

    @DisplayName("Creation command topic ok")
    @Test
    public void createCommandTopicOk() throws Exception {
        PowerMockito.spy(TopicManager.class);
        final Method m = Whitebox.getMethod(TopicManager.class, Collection.class, ApplicationConfig.class);
        PowerMockito.doNothing().when(TopicManager.class, m).withArguments(Matchers.any(Collection.class), Matchers.any(ApplicationConfig.class));

        final Map<String, String> commandTopic = new HashMap<>();
        commandTopic.put("topic" + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX, ApplicationConfig.COMMANDS_RECORD_TYPE);

        Exception ex = null;
        try {
            TopicManager.createTopics(commandTopic, new AppConfiguration().init());
        } catch (final Exception e) {
            ex = e;
        }
        Assertions.assertNull(ex);
    }

    @DisplayName("Creation event topic ok")
    @Test
    public void createEventopicOk() throws Exception {
        PowerMockito.spy(TopicManager.class);
        final Method m = Whitebox.getMethod(TopicManager.class, Collection.class, ApplicationConfig.class);
        PowerMockito.doNothing().when(TopicManager.class, m).withArguments(Matchers.any(Collection.class), Matchers.any(ApplicationConfig.class));

        final Map<String, String> commandTopic = new HashMap<>();
        commandTopic.put("topic" + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX, ApplicationConfig.EVENTS_RECORD_TYPE);

        Exception ex = null;
        try {
            TopicManager.createTopics(commandTopic, new AppConfiguration().init());
        } catch (final Exception e) {
            ex = e;
        }
        Assertions.assertNull(ex);
    }

    @DisplayName("Creation topic with config ok")
    @Test
    public void createTopicWithConfigOk() throws Exception {
        PowerMockito.spy(TopicManager.class);
        final Method m = Whitebox.getMethod(TopicManager.class, Collection.class, ApplicationConfig.class);
        PowerMockito.doNothing().when(TopicManager.class, m).withArguments(Matchers.any(Collection.class), Matchers.any(ApplicationConfig.class));

        final Map<String, String> commandTopic = new HashMap<>();
        commandTopic.put("topic" + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX, ApplicationConfig.EVENTS_RECORD_TYPE);

        Exception ex = null;
        final Map<String, Map<String, String>> topicName = new HashMap<>();
        topicName.put("topicName", TopicManager.configTypes.get(ApplicationConfig.CHANGELOG_RECORD_TYPE));
        try {
            TopicManager.createTopicsWithConfig(topicName, new AppConfiguration().init());
        } catch (final Exception e) {
            ex = e;
        }
        Assertions.assertNull(ex);
    }

}
