package com.bbva.ddd.domain;

import com.bbva.common.config.AppConfig;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.ddd.domain.annotations.Changelog;
import com.bbva.ddd.domain.annotations.Command;
import com.bbva.ddd.domain.annotations.Event;
import com.bbva.ddd.domain.changelogs.read.ChangelogHandlerContext;
import com.bbva.ddd.domain.commands.read.CommandHandlerContext;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.domain.consumers.HandlerContextImpl;
import com.bbva.ddd.domain.events.read.EventHandlerContext;
import com.bbva.ddd.util.AnnotationUtil;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Auto create the handler to manage all subscriptions in function of specific annotations as one of Command, Changelog or Event annotations.
 * The class with annotations need be annotated with Handler annotation.
 * If DomainBuilder is initialize only with configuration, by default it create a AutoConfiguredHandler
 * Example of methods annotation:
 * <pre>
 *  &#64;Handler
 *  public class MainHandler {
 *
 *      &#64;Command(baseName = "foo", commandAction = "create")
 *      public static void createFoo(final CommandRecord commandRecord) {
 *          AggregateFactory
 *              .create(FooAggregate.class, commandRecord.value(), commandRecord, (id, e) -&#62; {
 *                 if (e != null) {
 *                     e.printStackTrace();
 *                 } else {
 *                     System.out.println("Foo created!");
 *                 }
 *          });
 *      }
 *
 *  &#64;Event(baseName = "bar")
 *  public static void processBar(final EventRecord eventRecord) {
 *      HelperApplication
 *          .get()
 *          .sendEventTo("baz")
 *          .send("bar-producer", eventRecord.value, (id, e) -&#62; {
 *              if (e != null) {
 *                  e.printStackTrace();
 *              } else {
 *                  System.out.println("Event sent");
 *              }
 *      });
 *  }
 *
 *  &#64;Changelog(baseName = "foo")
 *  public static void processFooChangelog(final ChangelogRecord changelogRecord) {
 *      HelperApplication
 *          .get()
 *          .sendEventTo("bax")
 *          .send("foo-changelog-producer", changelogRecord.value, (id, e) -&#62; {
 *              if (e != null) {
 *                 e.printStackTrace();
 *              } else {
 *                  System.out.println("Event sent");
 *              }
 *          });
 *      }
 *  }
 *  </pre>
 */
public class AutoConfiguredHandler implements Handler {

    private static final Logger logger = LoggerFactory.getLogger(AutoConfiguredHandler.class);

    private static final List<String> commandsSubscribed = new ArrayList<>();
    private static final List<String> eventsSubscribed = new ArrayList<>();
    private static final List<String> dataChangelogsSubscribed = new ArrayList<>();

    private static final Map<String, Method> commandMethods = new HashMap<>();
    private static final Map<String, Method> eventMethods = new HashMap<>();
    private static final Map<String, Method> changelogMethods = new HashMap<>();

    /**
     * Constructor
     */
    public AutoConfiguredHandler() {
        final List<Class> handlers = AnnotationUtil.findAllAnnotatedClasses(com.bbva.ddd.domain.annotations.Handler.class);
        for (final Class handlerClass : handlers) {
            setAnnotatedActions(handlerClass);
        }
    }

    /**
     * Return the list of commands subscribed
     *
     * @return the list
     */
    @Override
    public List<String> commandsSubscribed() {
        return cleanList(commandsSubscribed);
    }

    /**
     * Return the list of events subscribed
     *
     * @return the list
     */
    @Override
    public List<String> eventsSubscribed() {
        return cleanList(eventsSubscribed);
    }

    /**
     * Return the list of changelogs subscribed
     *
     * @return the list
     */
    @Override
    public List<String> dataChangelogsSubscribed() {
        return cleanList(dataChangelogsSubscribed);
    }

    /**
     * Handle command produced
     *
     * @param commandHandlerContext command record
     */
    @Override
    public void processCommand(final CommandHandlerContext commandHandlerContext) {
        final String actionToExecute = commandHandlerContext.consumedRecord().topic().replace(AppConfig.COMMANDS_RECORD_NAME_SUFFIX, "") + "::" + getCommandAction(commandHandlerContext.consumedRecord());
        executeMethod(commandHandlerContext, commandMethods.get(actionToExecute));
    }

    /**
     * Handle event produced
     *
     * @param eventHandlerContext event record
     */
    @Override
    public void processEvent(final EventHandlerContext eventHandlerContext) {
        executeMethod(eventHandlerContext, eventMethods.get(eventHandlerContext.consumedRecord().topic().replace(AppConfig.EVENTS_RECORD_NAME_SUFFIX, "")));
    }

    /**
     * Handle changelog produced
     *
     * @param changelogHandlerContext changelog record
     */
    @Override
    public void processDataChangelog(final ChangelogHandlerContext changelogHandlerContext) {
        executeMethod(changelogHandlerContext, changelogMethods.get(changelogHandlerContext.consumedRecord().topic().replace(AppConfig.CHANGELOG_RECORD_NAME_SUFFIX, "")));
    }

    private static List<String> cleanList(final List<String> list) {
        final Set<String> uniqueCommands = new HashSet<>(list);
        list.clear();
        list.addAll(uniqueCommands);
        return list;
    }

    private static <C extends HandlerContextImpl> void executeMethod(final C context, final Method toExecuteMethod) {
        if (toExecuteMethod != null) {
            try {
                toExecuteMethod.invoke(null, context);
            } catch (final IllegalAccessException | InvocationTargetException e) {
                final String errorMsg = String.format("Error executing method: %s", toExecuteMethod.getName());
                logger.error(errorMsg, e);
                throw new ApplicationException(errorMsg, e);
            }
        } else {
            logger.info("Event not handled");
        }
    }

    private static String getCommandAction(final CommandRecord commandRecord) {
        return commandRecord.name();
    }

    private void setAnnotatedActions(Class<?> type) {
        while (type != Object.class) {
            final List<Method> allMethods = new ArrayList<>(Arrays.asList(type.getDeclaredMethods()));
            for (final Method method : allMethods) {
                if (method.isAnnotationPresent(Command.class)) {
                    final Command annotatedAction = method.getAnnotation(Command.class);
                    commandMethods.put(annotatedAction.baseName() + "::" + annotatedAction.commandAction(), method);
                    commandsSubscribed.add(annotatedAction.baseName() + AppConfig.COMMANDS_RECORD_NAME_SUFFIX);
                } else if (method.isAnnotationPresent(Event.class)) {
                    final Event annotatedEvent = method.getAnnotation(Event.class);
                    eventMethods.put(annotatedEvent.baseName(), method);
                    eventsSubscribed.add(annotatedEvent.baseName() + AppConfig.EVENTS_RECORD_NAME_SUFFIX);
                } else if (method.isAnnotationPresent(Changelog.class)) {
                    final Changelog annotatedChangelog = method.getAnnotation(Changelog.class);
                    changelogMethods.put(annotatedChangelog.baseName(), method);
                    dataChangelogsSubscribed.add(annotatedChangelog.baseName() + AppConfig.CHANGELOG_RECORD_NAME_SUFFIX);
                }
            }
            type = type.getSuperclass();
        }
    }
}
