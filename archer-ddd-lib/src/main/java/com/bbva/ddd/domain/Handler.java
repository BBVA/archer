package com.bbva.ddd.domain;

import com.bbva.common.consumers.CRecord;
import com.bbva.ddd.domain.changelogs.read.ChangelogRecord;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.domain.events.read.EventRecord;

import java.util.Collections;
import java.util.List;

/**
 * It can implements this interface and its methods to get events from the event store or annotate the handler class with
 * Handler annotation and methods with one of Command, Changelog or Event annotations. Example:
 * <pre>
 *  &#64;Handler
 *  public class MainHandler {
 *
 *     &#64;Command(baseName = "foo", commandAction = "create")
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
 *      &#64;Event(baseName = "bar")
 *      public static void processBar(final EventRecord eventRecord) {
 *          HelperApplication
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
 *      &#64;Changelog(baseName = "foo")
 *      public static void processFooChangelog(final ChangelogRecord changelogRecord) {
 *          HelperApplication
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
 * </pre>
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
     * @param commandMessage Command consumed from the event store
     */
    default void processCommand(final CommandRecord commandMessage) {
    }

    /**
     * This method is call when event messages arrived from event store
     *
     * @param eventMessage Event consumed from the event store
     */
    default void processEvent(final EventRecord eventMessage) {
    }

    /**
     * This method is call when changelog messages arrived from event store
     *
     * @param changelogMessage Changelog event consumed from the event store
     */
    default void processDataChangelog(final ChangelogRecord changelogMessage) {

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
