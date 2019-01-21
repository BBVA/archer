package com.bbva.ddd.domain;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.annotations.Changelog;
import com.bbva.ddd.domain.annotations.Command;
import com.bbva.ddd.domain.annotations.Event;
import com.bbva.ddd.domain.changelogs.read.ChangelogRecord;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.domain.events.read.EventRecord;
import com.bbva.ddd.util.AnnotationUtil;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;


public class AutoConfiguredHandler implements Handler {
    private static final LoggerGen logger = LoggerGenesis.getLogger(AutoConfiguredHandler.class.getName());

    private static final List<String> commandsSubscribed = new ArrayList<>();
    private static final List<String> eventsSubscribed = new ArrayList<>();
    private static final List<String> dataChangelogsSubscribed = new ArrayList<>();

    private static final Map<String, Method> commandMethods = new HashMap<>();
    private static final Map<String, Method> eventMethods = new HashMap<>();
    private static final Map<String, Method> changelogMethods = new HashMap<>();

    public AutoConfiguredHandler() {
        final List<Class> handlers = AnnotationUtil.findAllAnnotatedClasses(com.bbva.ddd.domain.annotations.Handler.class);
        for (final Class handlerClass : handlers) {
            setAnnotatedActions(handlerClass);
        }
    }

    public static List<Method> setAnnotatedActions(Class<?> type) {
        final List<Method> methods = new ArrayList<>();
        while (type != Object.class) {
            final List<Method> allMethods = new ArrayList<>(Arrays.asList(type.getDeclaredMethods()));
            for (final Method method : allMethods) {
                if (method.isAnnotationPresent(Command.class)) {
                    final Command annotatedAction = method.getAnnotation(Command.class);
                    final String baseName = !"".equals(annotatedAction.baseName()) ? annotatedAction.baseName() : getAggregateBaseName(annotatedAction.aggregateClass());
                    commandMethods.put(baseName + "::" + annotatedAction.commandAction(), method);
                    commandsSubscribed.add(baseName + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX);
                } else if (method.isAnnotationPresent(Event.class)) {
                    final Event annotatedEvent = method.getAnnotation(Event.class);
                    final String baseName = !"".equals(annotatedEvent.baseName()) ? annotatedEvent.baseName() : getAggregateBaseName(annotatedEvent.aggregateClass());
                    eventMethods.put(baseName, method);
                    eventsSubscribed.add(baseName + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX);
                } else if (method.isAnnotationPresent(Changelog.class)) {
                    final Changelog annotatedChangelog = method.getAnnotation(Changelog.class);
                    final String baseName = !"".equals(annotatedChangelog.baseName()) ? annotatedChangelog.baseName() : getAggregateBaseName(annotatedChangelog.aggregateClass());
                    changelogMethods.put(baseName, method);
                    dataChangelogsSubscribed.add(baseName + ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX);
                }
            }
            type = type.getSuperclass();
        }
        return methods;
    }

    private static String getAggregateBaseName(final Class aggregateClass) {
        return Aggregate.class.cast(aggregateClass.getAnnotation(Aggregate.class)).baseName();
    }

    @Override
    public List<String> commandsSubscribed() {
        return cleanList(commandsSubscribed);
    }

    @Override
    public List<String> eventsSubscribed() {
        return cleanList(eventsSubscribed);
    }

    @Override
    public List<String> dataChangelogsSubscribed() {
        return cleanList(dataChangelogsSubscribed);
    }

    @Override
    public void processCommand(final CommandRecord command) {
        try {
            final String actionToExecute = command.topic().replace(ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX, "") + "::" + getCommandAction(command);
            executeMethod(command, commandMethods.get(actionToExecute));
        } catch (final IllegalAccessException | InvocationTargetException e) {
            logger.error("Command not found", e);
        }
    }

    @Override
    public void processEvent(final EventRecord eventMessage) {
        try {
            executeMethod(eventMessage, eventMethods.get(eventMessage.topic().replace(ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX, "")));
        } catch (final IllegalAccessException | InvocationTargetException e) {
            logger.error("Event not found", e);
        }
    }

    @Override
    public void processDataChangelog(final ChangelogRecord changelogMessage) {
        try {
            executeMethod(changelogMessage, eventMethods.get(changelogMessage.topic().replace(ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX, "")));
        } catch (final IllegalAccessException | InvocationTargetException e) {
            logger.error("Changelog not found", e);
        }
    }

    private static List<String> cleanList(final List<String> list) {
        final Set<String> uniqueCommands = new HashSet<>(list);
        list.clear();
        list.addAll(uniqueCommands);
        return list;
    }

    private static <C extends CRecord> void executeMethod(final C command, final Method toExecuteMethod) throws IllegalAccessException, InvocationTargetException {
        if (toExecuteMethod != null) {
            toExecuteMethod.invoke(null, command);
        } else {
            logger.info("Event not handled");
        }
    }

    private static String getCommandAction(final CommandRecord commandRecord) {
        if (commandRecord.recordHeaders().find(CommandRecord.ACTION) != null) {
            return commandRecord.action();
        }
        return commandRecord.name();
    }
}
