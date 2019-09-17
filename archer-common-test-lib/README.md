Common lib test
==================

## Overview

This is the common test library to get up a complete kafka stack in memory and simulate the real event bus insfraestruture in the test environment.
It launchs a complete kafka stack and schema registry.

## Requirements

* [Java](https://www.java.com) >= 1.8

## Installation

Gradle
```text/plain
# build.gradle
dependencies {
	compile('bbva.ndb:lib-common-test:1.0.0-beta.1')
}
```

Maven
```text/plain
# pom.xml
<dependencies>
    <dependency>
        <groupId>bbva.ndb</groupId>
        <artifactId>lib-common-test</artifactId>
        <version>1.0.0-beta.1</version>
    </dependency>
</dependencies>
```


## Usage and examples
To use the kafka stack in mmemory you only need to extends of BaseItTest.

```java
public class MyTest extends BaseItTest {

```
And automatically in this class we have a populated ApplicationConfig with all configurations. At this moment we can consume, produce and do streams in front of memory stack.
```java
public class MyTest extends BaseItTest {

    @Before
    public void before() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new EventConsumer<Data>(10, Collections.singletonList("data_events"), testsConsumerCallback, appConfig));
    }
}
```
