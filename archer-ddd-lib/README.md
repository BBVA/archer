Domain Driven Design
==================

## Overview

ddd is an accommodation to simplify development of services with the Domain-Driven-Design paradigm  doing the applications more legibles and maintainable.

## Requirements

* [Java](https://www.java.com) >= 1.8

## Installation

Gradle
```text/plain
# build.gradle
dependencies {
	compile('bbva.ndb:ddd-lib:1.0.0-beta.2')
}
```

Maven
```text/plain
# pom.xml
<dependencies>
    <dependency>
        <groupId>bbva.ndb</groupId>
        <artifactId>ddd-lib</artifactId>
        <version>1.0.0-beta.2</version>
    </dependency>
</dependencies>
```

## Usage and examples

### Configuration

This library use [common-lib](../archer-common-lib/README.md) configuration system.

### Application layer

To simplify the develop of application layer the library provide a HelperApplication class, this allow send events and write commands, the two actions more used in this layer.

### Domain layer

This layer manage all event sourcing entities as Command, Events and Changelogs.
To initialize the domain you need to create and start the domain entity, that receive a handler implementation or not.
```java
Domain domain = DomainBuilder.create(handler,configuration);
//Add processors here
domain.start();
```
In the case that you not specify a handler implementation it is created with a AutoConfiguredHandler that is based in a annotation system. In this case you ned create a class with @Handler and in it create annotated methods to handle events. For example:
```java
@Handler
public class MyHandler { 
    
    @Command(commandAction = "action", source = "base")
    public void processCommand(CommandRecord command) {
        //Manage new command    
    }
    
    @Event("base")
    public void processEvent(EventRecord event) {
        //manage new event
    }
    
    @Changelog("base")
    public void processDataChangelog(ChangelogRecord changelog) {
        //Manage new changelog
    }
}
```

#### Aggregates
Aggregates are representations of the actual entity state. To manage it the library provide a hierarchical annotation system started in the domain start.
You can annotate a single entity with @Aggregate and if you want aggregate parent with childs you should combine it with @AggregateParent

For example:
```java
@Aggregate(baseName = FooAggregate.FOO)
public class FooAggregate extends SpecificAggregate<String, Foo> {

    public static final String FOO = "foos";

    public FooAggregate(final String id, final Foo record) {
        super(id, record);
    }

    public static String baseName() {
        return FOOS;
    }
}

```

#### Commands
The library provide a layer to manage reads and writes of command events.

To read a command topic:
```java
final ExecutorService executor = Executors.newFixedThreadPool(1);
final CommandConsumer commandConsumer = new CommandConsumer(1, Collections.singletonList("topic"), yourCallback, configuration);
executor.submit(commandConsumer);
```
and for produce:
```java
HelperApplication helperApplication = new HelperApplication(appConfig);
final CommandRecordMetadata recordMetadata =
    helperApplication.persistsCommandTo("entity_base_name")
        .create(entity, headers,
            (key, e) -> {
                if (e != null) {
                    logger.error("Error create user", e);
                }
        });
```

#### Events
The library provide a layer to manage reads and writes of events.

To read a event topic:
```java
final ExecutorService executor = Executors.newFixedThreadPool(1);
final EventConsumer eventConsumer = new EventConsumer(1, Collections.singletonList("event_topic"), yourCallback, configuration);
executor.submit(eventConsumer);
```
and for produce:
```java
HelperDomain helperDomain = new HelperDomain(appConfig);
helperDomain.sendEventTo("event_base_name")
    .send("producer_id", eventEntity, yourCallback);
```

#### Data changelogs
The changelog are the internal history of the entities.

To read a changelog topic:
```java
final ExecutorService executor = Executors.newFixedThreadPool(1);
final ChangelogConsumer changelogConsumer = new ChangelogConsumer(1, Collections.singletonList("changelog_topic"), yourCallback, configuration);
executor.submit(changelogConsumer);
```

To do any operation in a changelog topic (create, update and delete) the library provide a Repository class:
```java
final Repository repository = new Repository("topic", DataAggregate.class, configuration);

final RecordHeaders recordHeaders = new RecordHeaders();
recordHeaders.add(ChangelogHeaderType.UUID_KEY, new ByteArrayValue("key"));
recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("entityUUid"));
recordHeaders.add(ChangelogHeaderType.AGGREGATE_UUID_KEY, new ByteArrayValue("euuid"));
recordHeaders.add(ChangelogHeaderType.AGGREGATE_NAME_KEY, new ByteArrayValue("aggName"));
recordHeaders.add(ChangelogHeaderType.AGGREGATE_METHOD_KEY, new ByteArrayValue("aggMethod"));
recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(false));

final AggregateBase aggregateBase = repository.create("key", data, new CommandRecord("topic", 1, 1,
    new Date().getTime(), TimestampType.CREATE_TIME, null,
    data, recordHeaders), new DefaultProducerCallback());
```
