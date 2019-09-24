Archer framework
==================

## Overview

Archer is a research product conceived within BBVA New digital business - R&D that provides convenient libraries to develop a Event sourcing system. We provide simplicity, adaptability and efficiently to develop a event-driven architecture. 


Installs the packages in your project and can use this libraries,

 * Common
 * Data processorss
 * Domain driven design
 * Gateway
 * Logger
 * Common test


Getting Started
---------------

### Requirements

* [Java](https://www.java.com) >= 1.8

### Installation

Gradle
```text/plain
# build.gradle
dependencies {
	compile('bbva.ndb:lib-ddd:1.0.0-beta.1')
}
```

Maven
```text/plain
# pom.xml
<dependencies>
    <dependency>
        <groupId>bbva.ndb</groupId>
        <artifactId>lib-ddd</artifactId>
        <version>1.0.0-beta.1</version>
    </dependency>
</dependencies>
```


### Common

This is the common library to interact with the event bus. Provide producer and consumers of events, the infraestructure conectivity/usage and the utilities to serialize/deserialize the data.
[You see more about common](archer-common-lib/README.md)

### Data processorss

It provide a specific simply language to launch interactive queries in real-time to event store and facilitates the use of processing APIs.
[You see more about data processors](archer-data-processors-lib/README.md)

### Domain driven design

Simplify the develop of services with the pattern Domain-Driven-Design doing the applications more legibles and maintenables.

### Gateway

In teh Event Sourcing patter, the interaction with the external interfaces is carried out by gateways. This library facilitate the developt of gateways.

### Logger

Commons logger employing log4j to add appenders to event store for facilitate the exploitation of logs.
[You see more about logger lib](archer-log-lib/README.md)
### Common test

Utilities to get up a kafka ecosytem in memory to test the applications.
[You see more about common test](archer-common-test-lib/README.md)
