Logger lib
==================

## Overview

Commons logger employing log4j to add appenders to event store for facilitate the exploitation of logs.

## Requirements

* [Java](https://www.java.com) >= 1.8

## Installation

Gradle
```text/plain
# build.gradle
dependencies {
	compile('bbva.ndb:log-lib:1.0.0-beta.2')
}
```

Maven
```text/plain
# pom.xml
<dependencies>
    <dependency>
        <groupId>bbva.ndb</groupId>
        <artifactId>log-lib</artifactId>
        <version>1.0.0-beta.2</version>
    </dependency>
</dependencies>
```

## Configuration

This library use [common-lib](../archer-common-lib/README.md) configuration system.

## Usage
To enable logging appender you need to specify the base package, log level and the specific archerAppender setting a environment variable LOG_APPENDER_CONFIG, as the example:
```
export LOG_APPENDER_CONFIG=com.bbva=INFO, archerAppender
```

You can configure the baseName of the topic in which you want save the logs.
```
export LOG_SINK_NAME=log_events
```
