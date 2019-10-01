Data processors
==================

## Overview

Processing data is simplify by this library with the implementation of dataflow and sql processors. Too provide a interactive queries.

## Requirements

* [Java](https://www.java.com) >= 1.8

## Installation

Gradle
```text/plain
# build.gradle
dependencies {
	compile('bbva.ndb:data-processors-lib:1.0.0-beta.2')
}
```

Maven
```text/plain
# pom.xml
<dependencies>
    <dependency>
        <groupId>bbva.ndb</groupId>
        <artifactId>data-processors-lib</artifactId>
        <version>1.0.0-beta.2</version>
    </dependency>
</dependencies>
```


## Usage and examples

### Configuration

This library use [common-lib](../archer-common-lib/README.md) configuration system.

### Processors creation and initialization

To manage all processors and start its we have de DataProcessor class that you need create and add all processor before start the data processor.

For example, to add and start a data processor with a unique field state builder:
```java
DataProcessor
    .get()
    .add(storeName, new UniqueFieldStateBuilder<K, V, K1>(sourceStreamName, fieldPath, keyClass, resultKeyClass));
 
DataProcessor.get().start();
``` 

### Processors based on queries

You can create processors based on queries, for it you see the package com.bbva.dataprocessors.builders.sql in which the supported query types.

For example, to create and use a filtered queried stream you can do:
```java

final Map<String, String> columns = new HashMap<>();
columns.put("objectField", "STRUCT<anyFieldInObject VARCHAR>");
columns.put("otherField", "VARCHAR");

final CreateStreamQueryBuilder fooStream =
     QueryBuilderFactory
        .createStream("fooStreamName")
        .columns(columns)
        .with(
            QueryBuilderFactory
                .withProperties()
                .kafkaTopic("sourceStream")
                .valueFormat(WithPropertiesClauseBuilder.AVRO_FORMAT)
        );

final CreateStreamQueryBuilder fooFilteredStream =
    QueryBuilderFactory
        .createStream("fooFilteredStreamName")
        .with(
            QueryBuilderFactory
                .withProperties()
                .kafkaTopic(usersFilteredStreamName)
                .valueFormat(WithPropertiesClauseBuilder.AVRO_FORMAT)
        )
        .asSelect(
            QueryBuilderFactory
                .selectQuery()
                .addQueryFields(Collections.singletonList("objectField->anyFieldInObject as anyField"))
                .from(fooStream)
                .where("objectField->anyFieldInObject LIKE '%bar%'")
                );
DataProcessor
    .get()
    .add(fooFilteredStream);
 
DataProcessor.get().start();
``` 

### Processors based on data flows

you can create processors based on data flows, for it you see the package com.bbva.dataprocessors.builders.dataflow in which you see the different builders are available.

For example, to create and use a entity state processor you can do:
```java
DataProcessor
    .get()
    .add("stateName", new EntityStateBuilder<K, V>("foo", String.class));
 
DataProcessor.get().start();
``` 
