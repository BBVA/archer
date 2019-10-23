package com.bbva.ddd.domain.handlers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.ddd.domain.changelogs.consumers.ChangelogHandlerContext;
import com.bbva.ddd.domain.commands.consumers.CommandHandlerContext;
import com.bbva.ddd.domain.events.consumers.EventHandlerContext;

import java.util.Collections;
import java.util.List;

/**
 * It can implements this interface and its methods to get events from the event store or annotate the handler class with
 * Handler annotation and methods with one of Command, Changelog or Event annotations. Example:
 * <pre>{@code
 *  &#64;Handler
 *  public class MainHandler {
 *
 *     &#64;Command(source = "foo", commandAction = "create")
 *      public static void createFoo(final CommandRecord commandRecord) {
 *          AggregateFactory
 *              .create(FooAggregate.class, commandRecord.value(), commandRecord, (id, e) -&#62; {
 *                  if (e != null) {
 *                      e.printStackTrace();
 *                  } else {
 *                      System.out.println("Foo created!");
 *                  }
 *              });
 *      }
 *
 *      &#64;Event("bar")
 *      public static void processBar(final EventRecord eventRecord) {
 *          ApplicationHelper
 *              .get()
 *              .sendEventTo("baz")
 *              .send("bar-producer", eventRecord.value, (id, e) -&#62; {
 *                  if (e != null) {
 *                      e.printStackTrace();
 *                  } else {
 *                      System.out.println("Event sent");
 *                  }
 *              });
 *      }
 *
 *      &#64;Changelog("foo")
 *      public static void processFooChangelog(final ChangelogRecord changelogRecord) {
 *          ApplicationHelper
 *              .get()
 *              .sendEventTo("bax")
 *              .send("foo-changelog-producer", changelogRecord.value, (id, e) -&#62; {
 *                  if (e != null) {
 *                      e.printStackTrace();
 *                  } else {
 *                      System.out.println("Event sent");
 *                  }
 *              });
 *      }
 *  }
 * }</pre>
 */
public interface Handler {

    /**
     * This method is call when any message arrived from event store
     *
     * @param record Record consumed from the event store
     */
    default void processMessage(final CRecord record) {
    }

    /**
     * This method is call when command messages arrived from event store
     *
     * @param commandHandlerContext Command consumed from the event store
     */
    default void processCommand(final CommandHandlerContext commandHandlerContext) {
    }

    /**
     * This method is call when event messages arrived from event store
     *
     * @param eventHandlerContext Event consumed from the event store
     */
    default void processEvent(final EventHandlerContext eventHandlerContext) {
    }

    /**
     * This method is call when changelog messages arrived from event store
     *
     * @param changelogHandlerContext Changelog event consumed from the event store
     */
    default void processDataChangelog(final ChangelogHandlerContext changelogHandlerContext) {

    }

    /**
     * @return A list with the streams command base names to subscribe
     */
    default List<String> commandsSubscribed() {
        return Collections.emptyList();
    }

    /**
     * @return A list with the streams event base names to subscribe
     */
    default List<String> eventsSubscribed() {
        return Collections.emptyList();
    }

    /**
     * @return A list with the streams changelog base names to subscribe
     */
    default List<String> dataChangelogsSubscribed() {
        return Collections.emptyList();
    }

}
