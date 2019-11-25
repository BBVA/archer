Common lib
==================

## Overview

This is the common library to interact directly with the event bus. 

Provide classes for produce and consume events, too  simplify the infrastructure connectivity/usage and the utilities to serialize/deserialize the data from/to the bus.

## Requirements

* [Java](https://www.java.com) >= 1.8

## Installation

Gradle
```text/plain
# build.gradle
dependencies {
	compile('bbva.ndb:lib-common:1.0.0-beta.2')
}
```

Maven
```text/plain
# pom.xml
<dependencies>
    <dependency>
        <groupId>bbva.ndb</groupId>
        <artifactId>lib-common</artifactId>
        <version>1.0.0-beta.2</version>
    </dependency>
</dependencies>
```


## Usage and examples

### Configurations

To load basic configuration as [this](src/main/resources/common-config.yml) in your project need to annotate one class with @Config, but if you need override or add new configurations in the configuration should specify a yaml file with the extra configurations. For example:
``` java
@Config(file = "extra-config.yml")
public class ApplictionConfig {

```

In addition, you have the possibility to configure the library to interact with a secure kerberized stack with the @SecureConfig annotation. 
[You see more about secure run](kerberos.md)

Once we have annotated our class with the configuration, we have to start the configuration and store it in memory to share it with the rest of the system.
```java
AppConfig appConfig = ConfigBuilder().create();
```

with this initial configuration you need to specify a environment variables set required for the system. For example, you can configure in front a local environment:
```
export ARCHER_BOOTSTRAP_SERVERS=PLAINTEXT://localhost:9092
export ARCHER_SCHEMA_REGISTRY_URL=http://localhost:8081
export ARCHER_CONSUMER_GROUP_ID=local-app-group
export ARCHER_APPLICATION_SERVER=localhost:8080
export ARCHER_APPLICATION_NAME=archer-app
```

### Topic management

You can create and manage topics with the utility class TopicManager. For example:
```java
final Map<String, String> commandTopic = new HashMap<>();
commandTopic.put("baseName" + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX, ApplicationConfig.COMMANDS_RECORD_TYPE);
TopicManager.createTopics(commandTopic, appConfiguration);
```


### Consume
To consume events from a topic, the library provide a abstract class DefaultConsumer typed with a param that implement ConsumerContext to manage event context. Only you need implement the message creation.
- First, you need create a consumer:
```java
public class ExampleConsumer extends DefaultConsumer<ConsumerContextImpl> {

    @Override
    public ConsumerContextImpl context(final Producer producer, final CRecord record) {
        return new ConsumerContextImpl(producer, record);
    }

}

```
- When you create the consumer class you need to specify a callback function to manage the events read.
```java
final ExampleConsumer exampleConsumer = new ExampleConsumer(1, topics, callbackFunction, configuration);
```

### Produce
There is a producer default implementation DefaultProducer. In this case, you only need create a producer with the general config and it create/reuse for you the producer in all send operations. For example:
```java
Producer producer = new DefaultProducer(appConfig);
// 1. Add new record in the event store
final Future future = producer.send(new PRecord<>("topic", "key", "value", new RecordHeaders()), producerCallback);
// 2. Remove the record in the event store
final Future future = producer.send(new PRecord<>("topic", "key", null, new RecordHeaders()), producerCallback);
```
The send method return a future that is resolved with the ack is received.
