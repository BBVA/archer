Archer, an event-sourcing framework
===================================

## Overview

Archer is an innovative framework conceived within BBVA New Digital Businesses - R&D that provides convenient components to develop event sourcing systems. We provide simplicity, adaptability and efficiently to develop event-driven architectures.


The main components are:

 * Data Processors Library
 * Domain Driven Design Library
 * Gateway Library
 


Getting Started
---------------

### Requirements

* [Java](https://www.java.com) >= 1.8

### Common

This is the common library to interact directly with the event bus. 

Provide classes for produce and consume of events, too  simplify the infrastructure connectivity/usage and the utilities to serialize/deserialize the data from/to the bus.

[You see more about common](archer-common-lib/README.md)

### Data processors

Processing data is simplify by this library with the implementation of dataflow and sql processors, provide multiple builders to create processors. 

Provide a interactive queries system and Readable stores to launch queries to the data.

[You see more about data processors](archer-data-processors-lib/README.md)

### Domain driven design

ddd is an accommodation to simplify development of services with the Domain-Driven-Design paradigm  doing the applications more legibles and maintainable.

[You see more about ddd](archer-ddd-lib/README.md)

### Gateway

In the Event Sourcing pattern, the interaction with the external interfaces is carried out by gateways. This library facilitate the develop of gateways.

[You see more about gateway](archer-gateway-lib/README.md)

### Logger

Commons logger employing log4j to add appenders to event store for facilitate the exploitation of logs.
[You see more about logger lib](archer-log-lib/README.md)
### Common test

This is the common test library to get up a complete kafka stack in memory and simulate the real event bus infrastructure in the test environment.

It releases a complete kafka stack and schema registry.

[You see more about common test](archer-common-test-lib/README.md)
